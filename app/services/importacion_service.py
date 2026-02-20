"""
Import (Importacion) service layer.

Handles Excel and system-export (SIAF/SIGA) file uploads end-to-end:

1. Detect the file format using the existing ``app.parsers.detector`` module.
2. Dispatch to the format's registered parser (``BaseParser`` subclasses).
3. Bulk-insert valid records into the appropriate database tables.
4. Write a ``RegistroImportacion`` audit row.
5. Return an ``ImportacionUploadResponse`` summary to the calling router.

Bulk insert strategy
--------------------
- ``ProgramacionPresupuestal`` rows: keyed on (anio, ue_id, meta_id, clasificador_id).
- ``ProgramacionMensual`` rows: keyed on (programacion_presupuestal_id, mes).
  Formats 5A/5B resolve AO codes → presupuestal IDs via ActividadOperativa FK chain.
- ``ModificacionPresupuestal`` rows: from FORMATO_04 records.
- Master data (CUADRO_AO_META / TABLAS): upsert into master tables.
- ``FORMATO_5_RESUMEN``, ``ANEXO_01``: counted as processed (no dedicated table).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from sqlalchemy import extract, func
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models.actividad_operativa import ActividadOperativa
from app.models.clasificador_gasto import ClasificadorGasto
from app.models.meta_presupuestal import MetaPresupuestal
from app.models.modificacion_presupuestal import ModificacionPresupuestal
from app.models.programacion_mensual import ProgramacionMensual
from app.models.programacion_presupuestal import ProgramacionPresupuestal
from app.models.registro_importacion import RegistroImportacion
from app.models.unidad_ejecutora import UnidadEjecutora
from app.parsers.detector import detect_format
from app.parsers.base_parser import BaseParser, ParseResult
from app.schemas.common import FilterParams
from app.schemas.importacion import (
    EstadoFormatosResponse,
    FormatoEstadoItem,
    HistorialImportacion,
    ImportacionUploadResponse,
)
from app.services.file_storage import save_upload

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parser registry — add new parsers here as they are implemented
# ---------------------------------------------------------------------------

_PARSER_REGISTRY: dict[str, type[BaseParser]] = {}


def _register_available_parsers() -> None:
    """Populate ``_PARSER_REGISTRY`` with all implemented parser classes."""
    _parsers = [
        ("FORMATO_1", "app.parsers.formato1_parser", "Formato1Parser"),
        ("FORMATO_5A", "app.parsers.formato5a_parser", "Formato5AParser"),
        ("FORMATO_5B", "app.parsers.formato5b_parser", "Formato5BParser"),
        ("FORMATO_04", "app.parsers.formato04_parser", "Formato04Parser"),
        ("CUADRO_AO_META", "app.parsers.cuadro_ao_meta", "CuadroAoMetaParser"),
        ("TABLAS", "app.parsers.tablas_parser", "TablasParser"),
        ("FORMATO_2", "app.parsers.formato2_parser", "Formato2Parser"),
        ("FORMATO_3", "app.parsers.formato3_parser", "Formato3Parser"),
        ("FORMATO_5_RESUMEN", "app.parsers.formato5_resumen_parser", "Formato5ResumenParser"),
        ("ANEXO_01", "app.parsers.anexo01_parser", "Anexo01Parser"),
        ("SIAF", "app.parsers.siaf_parser", "SiafParser"),
        ("SIGA", "app.parsers.siga_parser", "SigaParser"),
    ]
    for key, module_path, class_name in _parsers:
        try:
            import importlib
            mod = importlib.import_module(module_path)
            _PARSER_REGISTRY[key] = getattr(mod, class_name)
        except Exception as exc:  # pragma: no cover
            logger.debug("%s not available: %s", class_name, exc)

    # Alias: DATOS_MAESTROS → same parser as CUADRO_AO_META
    if "CUADRO_AO_META" in _PARSER_REGISTRY:
        _PARSER_REGISTRY["DATOS_MAESTROS"] = _PARSER_REGISTRY["CUADRO_AO_META"]


_register_available_parsers()


# ---------------------------------------------------------------------------
# Bulk insert helpers
# ---------------------------------------------------------------------------


def _strip_internal_keys(rec: dict[str, Any]) -> dict[str, Any]:
    """Remove parser-internal keys (prefixed with ``_``) from a record dict."""
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def _resolve_codes_to_ids(
    db: Session, records: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Resolve ``ue_codigo``, ``meta_codigo``, ``clasificador_codigo`` to FK IDs.

    Auto-creates missing UE, Meta, and Clasificador entries so that data
    formats can be loaded independently of master data.
    """
    ue_map: dict[str, int] = {
        ue.codigo: ue.id for ue in db.query(UnidadEjecutora).all()
    }
    meta_map: dict[str, int] = {
        m.codigo: m.id for m in db.query(MetaPresupuestal).all()
    }
    clas_map: dict[str, int] = {
        c.codigo: c.id for c in db.query(ClasificadorGasto).all()
    }

    resolved: list[dict[str, Any]] = []
    warnings: list[str] = []

    # Extract a default anio from the first record that has one
    default_anio = 2026
    for r in records:
        a = r.get("anio")
        if a and int(a) > 2000:
            default_anio = int(a)
            break

    for rec in records:
        new_rec = dict(rec)

        # --- Resolve UE ---
        ue_code = new_rec.pop("ue_codigo", None)
        if ue_code and "ue_id" not in new_rec:
            ue_code_str = str(ue_code).strip()
            # Extract just the numeric code if format is "001 - NOMBRE"
            ue_code_clean = ue_code_str.split("-")[0].strip() if "-" in ue_code_str else ue_code_str
            ue_id = ue_map.get(ue_code_clean)
            if ue_id is None:
                # Auto-create
                ue_nombre = ue_code_str if "-" in ue_code_str else f"UE {ue_code_clean}"
                new_ue = UnidadEjecutora(
                    codigo=ue_code_clean,
                    nombre=ue_nombre,
                    sigla=f"UE-{ue_code_clean}",
                    tipo="CENTRAL" if ue_code_clean == "001" else "ODEI",
                    activo=True,
                )
                db.add(new_ue)
                db.flush()
                ue_map[ue_code_clean] = new_ue.id
                ue_id = new_ue.id
                warnings.append(f"UE '{ue_code_clean}' creada automaticamente.")
            new_rec["ue_id"] = ue_id
        elif "ue_id" not in new_rec:
            # No ue_codigo provided — get or create a default UE "001"
            if "001" not in ue_map:
                new_ue = UnidadEjecutora(
                    codigo="001", nombre="INEI SEDE CENTRAL",
                    sigla="INEI-CENTRAL", tipo="CENTRAL", activo=True,
                )
                db.add(new_ue)
                db.flush()
                ue_map["001"] = new_ue.id
                warnings.append("UE '001' (INEI SEDE CENTRAL) creada automaticamente.")
            new_rec["ue_id"] = ue_map["001"]

        # --- Resolve Meta ---
        meta_code = new_rec.pop("meta_codigo", None)
        if meta_code and "meta_id" not in new_rec:
            meta_code_str = str(meta_code).strip()
            meta_id = meta_map.get(meta_code_str)
            if meta_id is None:
                # Auto-create — needs ue_id
                new_meta = MetaPresupuestal(
                    codigo=meta_code_str,
                    descripcion=f"Meta {meta_code_str} (auto)",
                    ue_id=new_rec["ue_id"],
                    anio=new_rec.get("anio") or default_anio,
                    activo=True,
                )
                db.add(new_meta)
                db.flush()
                meta_map[meta_code_str] = new_meta.id
                meta_id = new_meta.id
                warnings.append(f"Meta '{meta_code_str}' creada automaticamente.")
            new_rec["meta_id"] = meta_id
        elif "meta_id" not in new_rec:
            # No meta_codigo provided — get or create default "0001"
            if "0001" not in meta_map:
                new_meta = MetaPresupuestal(
                    codigo="0001", descripcion="Meta General (auto)",
                    ue_id=new_rec["ue_id"], anio=default_anio, activo=True,
                )
                db.add(new_meta)
                db.flush()
                meta_map["0001"] = new_meta.id
                warnings.append("Meta '0001' creada automaticamente.")
            new_rec["meta_id"] = meta_map["0001"]

        # --- Resolve Clasificador ---
        clas_code = new_rec.pop("clasificador_codigo", None)
        if clas_code and "clasificador_id" not in new_rec:
            clas_code_str = str(clas_code).strip()
            clas_id = clas_map.get(clas_code_str)
            if clas_id is None:
                # Auto-create
                desc = new_rec.pop("descripcion", None) or f"Clasificador {clas_code_str}"
                parts = clas_code_str.split(".")
                tipo_gen = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else ""
                new_clas = ClasificadorGasto(
                    codigo=clas_code_str,
                    descripcion=desc,
                    tipo_generico=tipo_gen,
                )
                db.add(new_clas)
                db.flush()
                clas_map[clas_code_str] = new_clas.id
                clas_id = new_clas.id
                warnings.append(f"Clasificador '{clas_code_str}' creado automaticamente.")
            new_rec["clasificador_id"] = clas_id
        elif "clasificador_id" not in new_rec:
            warnings.append("Registro sin clasificador; fila omitida.")
            continue

        # Fix anio if 0 or missing
        if not new_rec.get("anio") or new_rec["anio"] == 0:
            new_rec["anio"] = default_anio

        # Remove extra keys not in the table
        new_rec.pop("descripcion", None)
        valid_cols = {c.name for c in ProgramacionPresupuestal.__table__.columns} - {"id"}
        new_rec = {k: v for k, v in new_rec.items() if k in valid_cols}
        resolved.append(new_rec)

    return resolved, warnings


def _bulk_insert_presupuestal(
    db: Session, records: list[dict[str, Any]], *, upsert: bool = False
) -> tuple[int, list[str]]:
    """Bulk-insert ProgramacionPresupuestal rows.

    When ``upsert=True`` (used by SIAF), existing rows are UPDATED with
    execution fields (certificado, compromiso_anual, devengado, girado, saldo).
    When ``upsert=False`` (default), exact duplicates are skipped.
    """
    inserted = 0
    updated = 0
    warnings: list[str] = []

    _EXECUTION_FIELDS = ("certificado", "compromiso_anual", "devengado", "girado", "saldo")

    for raw_rec in records:
        rec = _strip_internal_keys(raw_rec)
        anio = rec.get("anio")
        ue_id = rec.get("ue_id")
        meta_id = rec.get("meta_id")
        clasificador_id = rec.get("clasificador_id")

        existing = (
            db.query(ProgramacionPresupuestal)
            .filter(
                ProgramacionPresupuestal.anio == anio,
                ProgramacionPresupuestal.ue_id == ue_id,
                ProgramacionPresupuestal.meta_id == meta_id,
                ProgramacionPresupuestal.clasificador_id == clasificador_id,
            )
            .first()
        )
        if existing:
            if upsert:
                # Update execution fields from SIAF data
                changed = False
                for field in _EXECUTION_FIELDS:
                    new_val = rec.get(field)
                    if new_val is not None and new_val != 0:
                        old_val = getattr(existing, field, 0)
                        if float(new_val) != float(old_val or 0):
                            setattr(existing, field, new_val)
                            changed = True
                # Also update PIA/PIM if SIAF provides non-zero values
                for field in ("pia", "pim"):
                    new_val = rec.get(field)
                    if new_val is not None and new_val != 0:
                        old_val = getattr(existing, field, 0)
                        if float(new_val) != float(old_val or 0):
                            setattr(existing, field, new_val)
                            changed = True
                if changed:
                    updated += 1
                else:
                    warnings.append(
                        f"Registro existente sin cambios: anio={anio}, "
                        f"clasificador_id={clasificador_id}."
                    )
            else:
                warnings.append(
                    f"Fila duplicada omitida: anio={anio}, ue_id={ue_id}, "
                    f"meta_id={meta_id}, clasificador_id={clasificador_id}."
                )
            continue

        db.add(ProgramacionPresupuestal(**rec))
        inserted += 1

    db.flush()
    total = inserted + updated
    if updated:
        logger.info("_bulk_insert_presupuestal: %d inserted, %d updated", inserted, updated)
    return total, warnings


def _bulk_insert_mensual(
    db: Session, records: list[dict[str, Any]], *, upsert: bool = False
) -> tuple[int, list[str]]:
    """Bulk-insert ProgramacionMensual rows.

    When ``upsert=True`` (used by FORMATO_5B), existing rows are UPDATED
    with ejecutado and saldo values.  Otherwise duplicates are skipped.
    """
    inserted = 0
    updated = 0
    warnings: list[str] = []

    for raw_rec in records:
        rec = _strip_internal_keys(raw_rec)
        prog_id = rec.get("programacion_presupuestal_id")
        mes = rec.get("mes")

        if not prog_id:
            warnings.append(f"Registro mensual sin programacion_presupuestal_id (mes={mes}); omitido.")
            continue

        existing = (
            db.query(ProgramacionMensual)
            .filter(
                ProgramacionMensual.programacion_presupuestal_id == prog_id,
                ProgramacionMensual.mes == mes,
            )
            .first()
        )
        if existing:
            if upsert:
                changed = False
                for field in ("programado", "ejecutado", "saldo"):
                    new_val = rec.get(field)
                    if new_val is not None:
                        old_val = getattr(existing, field, 0)
                        if float(new_val) != float(old_val or 0):
                            setattr(existing, field, new_val)
                            changed = True
                if changed:
                    updated += 1
            else:
                warnings.append(f"Mes {mes} para programacion_id={prog_id} ya existe; omitido.")
            continue

        # Filter to valid columns
        valid_cols = {c.name for c in ProgramacionMensual.__table__.columns} - {"id"}
        clean_rec = {k: v for k, v in rec.items() if k in valid_cols}
        db.add(ProgramacionMensual(**clean_rec))
        inserted += 1

    db.flush()
    total = inserted + updated
    if updated:
        logger.info("_bulk_insert_mensual: %d inserted, %d updated", inserted, updated)
    return total, warnings


# ---------------------------------------------------------------------------
# A1: Resolve AO-based mensual records → programacion_presupuestal_id
# ---------------------------------------------------------------------------


def _resolve_ao_to_presupuestal_id(
    db: Session, records: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Resolve ``codigo_ao`` + ``ue_codigo`` + ``meta_codigo`` to a
    ``programacion_presupuestal_id`` FK for ProgramacionMensual insertion.

    Strategy:
    1. Find the ActividadOperativa by codigo_ceplan.
    2. Get the AO's ue_id and meta_id (from AO model or from the record codes).
    3. Find any ProgramacionPresupuestal row matching (anio, ue_id, meta_id).
       If multiple exist, pick the first one (they share the same budget line).
    4. If no ProgramacionPresupuestal exists, create a placeholder with 0 amounts
       so the mensual records have a valid FK.
    """
    warnings: list[str] = []
    resolved: list[dict[str, Any]] = []

    # Build lookup caches
    ao_map: dict[str, ActividadOperativa] = {
        ao.codigo_ceplan: ao
        for ao in db.query(ActividadOperativa).all()
    }
    ue_map: dict[str, int] = {
        ue.codigo: ue.id for ue in db.query(UnidadEjecutora).all()
    }
    meta_map: dict[str, int] = {
        m.codigo: m.id for m in db.query(MetaPresupuestal).all()
    }

    # Cache for presupuestal lookups: (anio, ue_id, meta_id) → presupuestal_id
    presup_cache: dict[tuple[int, int, int], int] = {}

    for rec in records:
        codigo_ao = str(rec.get("codigo_ao", "")).strip().upper()
        anio = rec.get("anio", 0)
        mes = rec.get("mes")

        # Resolve UE and Meta from record codes or AO model
        ue_id: int | None = None
        meta_id: int | None = None

        ao = ao_map.get(codigo_ao)
        if ao:
            ue_id = ao.ue_id
            meta_id = ao.meta_id

        # Fall back to codes in the record
        ue_codigo = str(rec.get("ue_codigo", "")).strip()
        meta_codigo = str(rec.get("meta_codigo", "")).strip()

        if not ue_id and ue_codigo:
            ue_id = ue_map.get(ue_codigo)
        if not meta_id and meta_codigo:
            meta_id = meta_map.get(meta_codigo)

        if not ue_id or not meta_id:
            warnings.append(
                f"AO '{codigo_ao}' mes {mes}: no se pudo resolver UE/Meta; omitido."
            )
            continue

        # Find or create ProgramacionPresupuestal
        cache_key = (int(anio), int(ue_id), int(meta_id))
        presup_id = presup_cache.get(cache_key)

        if presup_id is None:
            # Try to find existing
            row = (
                db.query(ProgramacionPresupuestal.id)
                .filter(
                    ProgramacionPresupuestal.anio == anio,
                    ProgramacionPresupuestal.ue_id == ue_id,
                    ProgramacionPresupuestal.meta_id == meta_id,
                )
                .first()
            )
            if row:
                presup_id = row[0]
            else:
                # Pick the first clasificador to create a placeholder
                first_clas = db.query(ClasificadorGasto.id).first()
                if not first_clas:
                    warnings.append(
                        f"AO '{codigo_ao}': no hay clasificadores en BD; "
                        "cargue el formato TABLAS primero."
                    )
                    continue
                placeholder = ProgramacionPresupuestal(
                    anio=anio,
                    ue_id=ue_id,
                    meta_id=meta_id,
                    clasificador_id=first_clas[0],
                    pia=0, pim=0, certificado=0, compromiso_anual=0,
                    devengado=0, girado=0, saldo=0,
                )
                db.add(placeholder)
                db.flush()
                presup_id = placeholder.id
                warnings.append(
                    f"AO '{codigo_ao}': ProgramacionPresupuestal placeholder "
                    f"creada (id={presup_id}). Cargue Formato 1/2 para datos reales."
                )

            presup_cache[cache_key] = presup_id

        resolved.append({
            "programacion_presupuestal_id": presup_id,
            "mes": mes,
            "programado": rec.get("programado", 0),
            "ejecutado": rec.get("ejecutado", 0),
            "saldo": rec.get("saldo", 0),
        })

    return resolved, warnings


# ---------------------------------------------------------------------------
# A2: Master data upsert (CUADRO_AO_META / TABLAS)
# ---------------------------------------------------------------------------


def _bulk_upsert_maestros(
    db: Session, formato: str, records: list[dict[str, Any]]
) -> tuple[int, list[str]]:
    """Upsert master data records from CUADRO_AO_META or TABLAS parsers."""
    if formato in ("CUADRO_AO_META", "DATOS_MAESTROS"):
        return _upsert_cuadro_ao_meta(db, records)
    if formato == "TABLAS":
        return _upsert_tablas(db, records)
    return 0, [f"Formato maestro '{formato}' no tiene lógica de upsert."]


def _upsert_cuadro_ao_meta(
    db: Session, records: list[dict[str, Any]]
) -> tuple[int, list[str]]:
    """Upsert UE, Meta, and AO records from CUADRO_AO_META parser output."""
    inserted = 0
    warnings: list[str] = []

    # Separate by _type
    ue_recs = [r for r in records if r.get("_type") == "unidad_ejecutora"]
    meta_recs = [r for r in records if r.get("_type") == "meta_presupuestal"]
    ao_recs = [r for r in records if r.get("_type") == "actividad_operativa"]

    # 1. Upsert UnidadEjecutora
    for rec in ue_recs:
        codigo = str(rec.get("codigo", "")).strip()
        if not codigo:
            continue
        existing = db.query(UnidadEjecutora).filter(UnidadEjecutora.codigo == codigo).first()
        if existing:
            existing.nombre = rec.get("nombre", existing.nombre)
            existing.sigla = rec.get("sigla", existing.sigla)
            existing.tipo = rec.get("tipo", existing.tipo)
            existing.activo = True
        else:
            db.add(UnidadEjecutora(
                codigo=codigo,
                nombre=rec.get("nombre", ""),
                sigla=rec.get("sigla", ""),
                tipo=rec.get("tipo", "ODEI"),
                activo=True,
            ))
            inserted += 1
    db.flush()

    # Rebuild UE map after flush
    ue_map: dict[str, int] = {
        ue.codigo: ue.id for ue in db.query(UnidadEjecutora).all()
    }

    # 2. Upsert MetaPresupuestal
    for rec in meta_recs:
        codigo = str(rec.get("codigo", "")).strip()
        ue_codigo = str(rec.get("ue_codigo", "")).strip()
        anio = rec.get("anio", 0)
        if not codigo:
            continue
        ue_id = ue_map.get(ue_codigo)
        if not ue_id:
            warnings.append(f"Meta '{codigo}': UE '{ue_codigo}' no encontrada; omitida.")
            continue
        existing = (
            db.query(MetaPresupuestal)
            .filter(MetaPresupuestal.codigo == codigo, MetaPresupuestal.ue_id == ue_id)
            .first()
        )
        if existing:
            existing.descripcion = rec.get("descripcion", existing.descripcion)
            existing.sec_funcional = rec.get("sec_funcional", existing.sec_funcional)
            existing.activo = True
        else:
            db.add(MetaPresupuestal(
                codigo=codigo,
                descripcion=rec.get("descripcion", ""),
                sec_funcional=rec.get("sec_funcional"),
                ue_id=ue_id,
                anio=anio or 2026,
                activo=True,
            ))
            inserted += 1
    db.flush()

    # Rebuild maps
    meta_map: dict[tuple[str, int], int] = {
        (m.codigo, m.ue_id): m.id for m in db.query(MetaPresupuestal).all()
    }

    # 3. Upsert ActividadOperativa
    for rec in ao_recs:
        codigo_ceplan = str(rec.get("codigo_ceplan", "")).strip().upper()
        if not codigo_ceplan:
            continue
        ue_codigo = str(rec.get("ue_codigo", "")).strip()
        meta_codigo = str(rec.get("meta_codigo", "")).strip()
        ue_id = ue_map.get(ue_codigo)
        meta_id = meta_map.get((meta_codigo, ue_id)) if ue_id else None

        existing = (
            db.query(ActividadOperativa)
            .filter(ActividadOperativa.codigo_ceplan == codigo_ceplan)
            .first()
        )
        if existing:
            existing.nombre = rec.get("nombre", existing.nombre)
            existing.oei = rec.get("oei", existing.oei)
            existing.aei = rec.get("aei", existing.aei)
            if ue_id:
                existing.ue_id = ue_id
            if meta_id:
                existing.meta_id = meta_id
            existing.activo = True
        else:
            db.add(ActividadOperativa(
                codigo_ceplan=codigo_ceplan,
                nombre=rec.get("nombre", ""),
                oei=rec.get("oei"),
                aei=rec.get("aei"),
                meta_id=meta_id,
                ue_id=ue_id,
                anio=rec.get("anio", 0) or 2026,
                activo=True,
            ))
            inserted += 1
    db.flush()

    total = inserted
    logger.info(
        "_upsert_cuadro_ao_meta: inserted=%d (UEs=%d, Metas=%d, AOs=%d)",
        total, len(ue_recs), len(meta_recs), len(ao_recs),
    )
    return len(records), warnings


def _upsert_tablas(
    db: Session, records: list[dict[str, Any]]
) -> tuple[int, list[str]]:
    """Upsert ClasificadorGasto records from TABLAS parser output."""
    inserted = 0
    warnings: list[str] = []

    for rec in records:
        codigo = str(rec.get("codigo", "")).strip()
        if not codigo:
            continue
        existing = db.query(ClasificadorGasto).filter(ClasificadorGasto.codigo == codigo).first()
        if existing:
            existing.descripcion = rec.get("descripcion", existing.descripcion)
            existing.tipo_generico = rec.get("tipo_generico", existing.tipo_generico)
        else:
            db.add(ClasificadorGasto(
                codigo=codigo,
                descripcion=rec.get("descripcion", ""),
                tipo_generico=rec.get("tipo_generico"),
            ))
            inserted += 1

    db.flush()
    logger.info("_upsert_tablas: %d new clasificadores inserted", inserted)
    return len(records), warnings


# ---------------------------------------------------------------------------
# A3: Modificacion presupuestal bulk insert
# ---------------------------------------------------------------------------


def _bulk_insert_modificaciones(
    db: Session, records: list[dict[str, Any]]
) -> tuple[int, list[str]]:
    """Insert ModificacionPresupuestal rows from FORMATO_04 parser output."""
    inserted = 0
    warnings: list[str] = []

    ue_map: dict[str, int] = {
        ue.codigo: ue.id for ue in db.query(UnidadEjecutora).all()
    }
    clas_map: dict[str, int] = {
        c.codigo: c.id for c in db.query(ClasificadorGasto).all()
    }

    valid_cols = {c.name for c in ModificacionPresupuestal.__table__.columns} - {"id"}

    for rec in records:
        clean = _strip_internal_keys(rec)

        # Resolve ue_codigo → ue_id
        ue_codigo = clean.pop("ue_codigo", None)
        if ue_codigo and "ue_id" not in clean:
            ue_id = ue_map.get(str(ue_codigo).strip())
            if ue_id:
                clean["ue_id"] = ue_id
            else:
                warnings.append(f"Modif: UE '{ue_codigo}' no encontrada.")

        # Resolve clasificador_codigo → clasificador_id
        clas_codigo = clean.pop("clasificador_codigo", None)
        if clas_codigo and "clasificador_id" not in clean:
            clas_id = clas_map.get(str(clas_codigo).strip())
            if clas_id:
                clean["clasificador_id"] = clas_id
            else:
                warnings.append(f"Modif: Clasificador '{clas_codigo}' no encontrado.")

        # Remove extra fields not in model
        clean = {k: v for k, v in clean.items() if k in valid_cols}
        db.add(ModificacionPresupuestal(**clean))
        inserted += 1

    db.flush()
    logger.info("_bulk_insert_modificaciones: %d records inserted", inserted)
    return inserted, warnings


# ---------------------------------------------------------------------------
# Audit log writer
# ---------------------------------------------------------------------------


def _write_audit_log(
    db: Session,
    *,
    formato: str,
    archivo_nombre: str,
    usuario_id: int,
    usuario_username: str,
    ue_sigla: str | None,
    registros_ok: int,
    registros_error: int,
    errors: list[str],
    warnings: list[str],
) -> None:
    """Persist a ``RegistroImportacion`` row as an import audit record."""
    if registros_error == 0:
        estado = "EXITOSO"
    elif registros_ok > 0:
        estado = "PARCIAL"
    else:
        estado = "FALLIDO"

    db.add(
        RegistroImportacion(
            formato=formato,
            archivo_nombre=archivo_nombre,
            fecha=datetime.now(timezone.utc),
            usuario_id=usuario_id,
            usuario_username=usuario_username,
            ue_sigla=ue_sigla,
            registros_ok=registros_ok,
            registros_error=registros_error,
            estado=estado,
            errors_json=json.dumps(errors, ensure_ascii=False) if errors else None,
            warnings_json=json.dumps(warnings, ensure_ascii=False) if warnings else None,
        )
    )


# ---------------------------------------------------------------------------
# DB table dispatch for parser output
# ---------------------------------------------------------------------------

#: Formats whose ``ParseResult.records`` map to ProgramacionPresupuestal rows.
_PRESUPUESTAL_FORMATS: frozenset[str] = frozenset(
    {"FORMATO_1", "FORMATO_2", "FORMATO_3", "SIAF"}
)

#: Formats whose records carry _type="programacion_mensual" with codigo_ao
#: and need AO→presupuestal_id resolution.
_MENSUAL_FORMATS: frozenset[str] = frozenset(
    {"FORMATO_5A", "FORMATO_5B"}
)

#: Master data formats → upsert into master tables
_MAESTRO_FORMATS: frozenset[str] = frozenset(
    {"CUADRO_AO_META", "DATOS_MAESTROS", "TABLAS"}
)

#: FORMATO_04 → modificacion_presupuestal
_MODIFICACION_FORMATS: frozenset[str] = frozenset({"FORMATO_04"})

#: Formats that produce records but have no dedicated table.
#: They are counted as processed in the audit log.
_PASSTHROUGH_FORMATS: frozenset[str] = frozenset(
    {"FORMATO_5_RESUMEN", "ANEXO_01", "SIGA"}
)


def _persist_parse_result(
    db: Session, formato: str, result: ParseResult
) -> tuple[int, list[str]]:
    """Route parser output to the correct table and insert valid rows.

    Handles mixed-type records (e.g. FORMATO_1 emitting both
    programacion_presupuestal and programacion_mensual records).
    """
    all_records = result.records
    total_inserted = 0
    all_warnings: list[str] = []

    # --- A5: Separate records by _type if mixed ---
    presup_records = [r for r in all_records if r.get("_type") == "programacion_presupuestal"]
    mensual_records = [r for r in all_records if r.get("_type") == "programacion_mensual"]
    modif_records = [r for r in all_records if r.get("_type") == "modificacion_presupuestal"]
    # Master data types
    maestro_types = {"unidad_ejecutora", "meta_presupuestal", "actividad_operativa", "clasificador_gasto"}
    maestro_records = [r for r in all_records if r.get("_type") in maestro_types]
    # Records with no _type (legacy parsers that don't tag records)
    untyped_records = [r for r in all_records if not r.get("_type")]

    # --- Process by format category ---

    # Master data formats
    if formato in _MAESTRO_FORMATS and (maestro_records or all_records):
        recs_to_use = maestro_records if maestro_records else all_records
        ins, warns = _bulk_upsert_maestros(db, formato, recs_to_use)
        total_inserted += ins
        all_warnings.extend(warns)
        return total_inserted, all_warnings

    # FORMATO_04 → modificacion_presupuestal
    if formato in _MODIFICACION_FORMATS:
        recs = modif_records if modif_records else all_records
        ins, warns = _bulk_insert_modificaciones(db, recs)
        total_inserted += ins
        all_warnings.extend(warns)
        return total_inserted, all_warnings

    # Presupuestal formats (FORMATO_1/2/3, SIAF)
    if formato in _PRESUPUESTAL_FORMATS:
        # Process presupuestal records (untyped or explicitly typed)
        presup_to_insert = presup_records if presup_records else untyped_records if untyped_records else all_records
        if presup_to_insert:
            resolved, resolve_warns = _resolve_codes_to_ids(db, presup_to_insert)
            # SIAF provides execution data — upsert to update existing rows
            is_siaf = formato == "SIAF"
            ins, insert_warns = _bulk_insert_presupuestal(db, resolved, upsert=is_siaf)
            total_inserted += ins
            all_warnings.extend(resolve_warns + insert_warns)

        # Also process any mensual records embedded in the same parse result
        if mensual_records:
            resolved_m, warns_m = _resolve_ao_to_presupuestal_id(db, mensual_records)
            ins_m, ins_warns_m = _bulk_insert_mensual(db, resolved_m)
            total_inserted += ins_m
            all_warnings.extend(warns_m + ins_warns_m)

        return total_inserted, all_warnings

    # Mensual formats (FORMATO_5A/5B) — records have codigo_ao, need resolution
    if formato in _MENSUAL_FORMATS:
        resolved, resolve_warns = _resolve_ao_to_presupuestal_id(db, all_records)
        # 5B has ejecutado data — upsert to update existing 5A rows
        is_5b = formato == "FORMATO_5B"
        ins, insert_warns = _bulk_insert_mensual(db, resolved, upsert=is_5b)
        total_inserted += ins
        all_warnings.extend(resolve_warns + insert_warns)
        return total_inserted, all_warnings

    # Passthrough formats — count records but don't persist
    if formato in _PASSTHROUGH_FORMATS and all_records:
        logger.info(
            "_persist: formato='%s' %d records counted as processed (no table).",
            formato, len(all_records),
        )
        return len(all_records), [
            f"Formato '{formato}': {len(all_records)} registros leídos correctamente."
        ]

    # Unknown formats with records
    if all_records:
        logger.info(
            "_persist: formato='%s' has %d records — no table mapping.",
            formato, len(all_records),
        )
        return len(all_records), [
            f"Formato '{formato}': {len(all_records)} registros leídos "
            "correctamente (almacenamiento en tabla específica pendiente)."
        ]

    return 0, []


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


async def process_upload(
    db: Session,
    file: UploadFile,
    user_id: int,
    username: str,
    declared_format: str | None = None,
) -> ImportacionUploadResponse:
    """Process an uploaded Excel or system-export file end-to-end."""
    # 1. Read bytes
    raw: bytes = await file.read()
    filename: str = file.filename or "upload.xlsx"

    if not raw:
        raise ValueError("El archivo está vacío.")

    # 1b. Save uploaded file to disk
    try:
        settings = get_settings()
        saved_path = save_upload(raw, filename, settings.UPLOADS_DIR, username=username)
        logger.info("File saved to: %s", saved_path)
    except Exception as exc:
        logger.warning("Could not save upload to disk: %s", exc)

    # 2. Detect format
    auto_formato = detect_format(raw)
    if declared_format == "DATOS_MAESTROS":
        # Allow auto-detection to distinguish CUADRO_AO_META vs TABLAS
        formato = auto_formato if auto_formato in ("CUADRO_AO_META", "TABLAS") else declared_format
    else:
        formato = declared_format if declared_format else auto_formato

    logger.info(
        "process_upload: file='%s' auto='%s' effective='%s' user='%s'",
        filename, auto_formato, formato, username,
    )

    # 3. Detect UE from filename (best-effort)
    ue_sigla: str | None = None
    filename_upper = filename.upper()
    for ue in db.query(UnidadEjecutora).filter(UnidadEjecutora.activo.is_(True)).all():
        if ue.sigla and ue.sigla.upper() in filename_upper:
            ue_sigla = ue.sigla
            break

    # 4. Parse
    errors: list[str] = []
    warnings: list[str] = []
    result: ParseResult = ParseResult(format_name=formato)

    parser_cls = _PARSER_REGISTRY.get(formato)
    if parser_cls is None:
        errors.append(
            f"Formato '{formato}' no tiene un parser implementado. "
            "Verifique el archivo o contacte al administrador."
        )
    else:
        try:
            parser_instance: BaseParser = parser_cls(raw)
            result = parser_instance.parse()
            errors.extend(result.errors)
            warnings.extend(result.warnings)
            if not ue_sigla and result.metadata.get("ue_sigla"):
                ue_sigla = result.metadata["ue_sigla"]
        except NotImplementedError as exc:
            errors.append(str(exc))
        except Exception as exc:
            logger.exception("Unexpected parser error for format '%s'", formato)
            errors.append(f"Error inesperado durante el análisis del archivo: {exc}")

    # 5. Persist valid records
    registros_ok = 0
    if result.records and not errors:
        try:
            registros_ok, insert_warnings = _persist_parse_result(db, formato, result)
            warnings.extend(insert_warnings)
        except Exception as exc:
            db.rollback()
            logger.exception("DB insert failed for format '%s'", formato)
            errors.append(f"Error al insertar registros en la base de datos: {exc}")
            registros_ok = 0

    registros_error = len(result.errors)

    # 6. Audit log + commit
    try:
        _write_audit_log(
            db,
            formato=formato,
            archivo_nombre=filename,
            usuario_id=user_id,
            usuario_username=username,
            ue_sigla=ue_sigla,
            registros_ok=registros_ok,
            registros_error=registros_error,
            errors=errors,
            warnings=warnings,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.exception("Failed to write audit log for import '%s'", filename)
        raise RuntimeError(f"Error al guardar el registro de importación: {exc}") from exc

    # 7. Build response
    total_rows_read = len(result.records) + registros_error
    meta: dict[str, Any] = {
        "archivo": filename,
        "ue_detectada": ue_sigla,
        "total_filas_leidas": total_rows_read,
        **result.metadata,
    }

    return ImportacionUploadResponse(
        formato_detectado=formato,
        registros_validos=registros_ok,
        registros_error=registros_error,
        warnings=warnings,
        errors=errors,
        metadata=meta,
    )


# ---------------------------------------------------------------------------
# History query
# ---------------------------------------------------------------------------


def _func_year(column: Any) -> Any:
    return extract("year", column)


def get_historial(
    db: Session,
    filters: FilterParams,
) -> list[HistorialImportacion]:
    """Return the import history list, most-recent first."""
    q = db.query(RegistroImportacion)

    if filters.anio is not None:
        q = q.filter(_func_year(RegistroImportacion.fecha) == filters.anio)

    records = q.order_by(RegistroImportacion.fecha.desc()).all()

    result: list[HistorialImportacion] = []
    for rec in records:
        result.append(
            HistorialImportacion(
                id=rec.id,
                formato=rec.formato,
                archivo_nombre=rec.archivo_nombre,
                fecha=rec.fecha,
                usuario=rec.usuario_username,
                ue=rec.ue_sigla,
                registros_ok=rec.registros_ok,
                registros_error=rec.registros_error,
                estado=rec.estado,
            )
        )

    logger.debug("get_historial: %d records returned", len(result))
    return result


# ---------------------------------------------------------------------------
# C2: Estado de formatos (format status dashboard)
# ---------------------------------------------------------------------------

_ESTADO_CATALOG: list[dict[str, Any]] = [
    {
        "formato": "CUADRO_AO_META",
        "plantilla_key": "cuadro_ao_meta",
        "nombre": "Cuadro AO-META",
        "descripcion": "Datos maestros: 85 AOs, UEs, metas presupuestales",
        "categoria": "DATOS_MAESTROS",
        "es_requerido": True,
        "tiene_plantilla": True,
        "impacto": "Define la jerarquia base UE - Meta - AO para todo el sistema",
        "upload_endpoint": "/api/importacion/datos-maestros",
    },
    {
        "formato": "TABLAS",
        "plantilla_key": "tablas",
        "nombre": "Tablas - Clasificadores",
        "descripcion": "569 clasificadores de gasto SIAF",
        "categoria": "DATOS_MAESTROS",
        "es_requerido": True,
        "tiene_plantilla": True,
        "impacto": "Codigos de gasto para programacion presupuestal",
        "upload_endpoint": "/api/importacion/datos-maestros",
    },
    {
        "formato": "FORMATO_1",
        "plantilla_key": "formato1",
        "nombre": "Formato 1 - Programacion Presupuestal",
        "descripcion": "Programacion anual con desglose mensual por clasificador",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": True,
        "tiene_plantilla": True,
        "impacto": "Dashboard Presupuesto: KPIs PIA/PIM/Certificado/Devengado",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "FORMATO_2",
        "plantilla_key": "formato2",
        "nombre": "Formato 2 - Programacion por Tareas",
        "descripcion": "Programacion a nivel de tarea por clasificador",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Detalle de tareas en drill-down presupuestal",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "FORMATO_3",
        "plantilla_key": "formato3",
        "nombre": "Formato 3 - Tareas con Justificacion",
        "descripcion": "Tareas con justificacion y detalle de programacion",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Detalle justificativo en drill-down",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "FORMATO_04",
        "plantilla_key": "formato04",
        "nombre": "Formato 04 - Modificaciones Presupuestales",
        "descripcion": "Habilitaciones y habilitadas de credito presupuestario",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Seguimiento de transferencias presupuestales PIM",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "FORMATO_5A",
        "plantilla_key": "formato5a",
        "nombre": "Formato 5.A - Programacion AO",
        "descripcion": "Programacion mensual por actividad operativa (solo programado)",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": True,
        "tiene_plantilla": True,
        "impacto": "Dashboard AO: programacion mensual, semaforo de ejecucion",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "FORMATO_5B",
        "plantilla_key": "formato5b",
        "nombre": "Formato 5.B - Ejecucion AO",
        "descripcion": "Programado vs ejecutado por AO y mes (45 columnas triple)",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": True,
        "tiene_plantilla": True,
        "impacto": "Dashboard AO: ejecucion real, semaforo, alertas sub-ejecucion",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "FORMATO_5_RESUMEN",
        "plantilla_key": "formato5_resumen",
        "nombre": "Formato 5 Resumen",
        "descripcion": "Resumen consolidado de actividades operativas",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Vista resumen consolidada de AOs",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "ANEXO_01",
        "plantilla_key": "anexo01",
        "nombre": "Anexo 01 - Recursos Humanos",
        "descripcion": "Datos de personal: DNI, remuneraciones, certificaciones",
        "categoria": "FORMATOS_DDNNTT",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Componente de gastos de personal",
        "upload_endpoint": "/api/importacion/formatos",
    },
    {
        "formato": "SIAF",
        "plantilla_key": "siaf",
        "nombre": "SIAF - Sistema Financiero",
        "descripcion": "Exportacion del Sistema Integrado de Administracion Financiera",
        "categoria": "SISTEMAS_EXTERNOS",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Actualiza ejecucion presupuestal real (devengado, girado)",
        "upload_endpoint": "/api/importacion/siaf",
    },
    {
        "formato": "SIGA",
        "plantilla_key": "siga",
        "nombre": "SIGA - Sistema Logistico",
        "descripcion": "Exportacion del Sistema Integrado de Gestion Administrativa",
        "categoria": "SISTEMAS_EXTERNOS",
        "es_requerido": False,
        "tiene_plantilla": True,
        "impacto": "Informacion de requerimientos y ordenes logisticas",
        "upload_endpoint": "/api/importacion/siga",
    },
]


def get_estado_formatos(db: Session) -> EstadoFormatosResponse:
    """Query the last import record per format and compose a status dashboard."""
    settings = get_settings()

    # Get latest import per formato using a subquery
    from sqlalchemy import desc
    latest_imports: dict[str, RegistroImportacion] = {}
    for fmt_info in _ESTADO_CATALOG:
        fmt_key = fmt_info["formato"]
        row = (
            db.query(RegistroImportacion)
            .filter(RegistroImportacion.formato == fmt_key)
            .order_by(desc(RegistroImportacion.fecha))
            .first()
        )
        if row:
            latest_imports[fmt_key] = row

    formatos: list[FormatoEstadoItem] = []
    total = len(_ESTADO_CATALOG)
    cargados_exitosos = 0
    cargados_parcial = 0
    sin_cargar = 0
    requeridos_faltantes = 0

    for fmt_info in _ESTADO_CATALOG:
        fmt_key = fmt_info["formato"]
        latest = latest_imports.get(fmt_key)

        # Check plantilla exists using the correct file key
        tiene_plantilla = fmt_info["tiene_plantilla"]
        if tiene_plantilla:
            pkey = fmt_info.get("plantilla_key", fmt_key.lower())
            plantilla_path = settings.PLANTILLAS_DIR / f"plantilla_{pkey}.xlsx"
            tiene_plantilla = plantilla_path.exists()

        if latest:
            estado = latest.estado  # EXITOSO, PARCIAL, FALLIDO
            ultima_carga = latest.fecha.isoformat() if latest.fecha else None
            registros_ok = latest.registros_ok
            usuario = latest.usuario_username
        else:
            estado = "SIN_CARGAR"
            ultima_carga = None
            registros_ok = 0
            usuario = None

        if estado == "EXITOSO":
            cargados_exitosos += 1
        elif estado == "PARCIAL":
            cargados_parcial += 1
        elif estado == "SIN_CARGAR":
            sin_cargar += 1
            if fmt_info["es_requerido"]:
                requeridos_faltantes += 1

        formatos.append(FormatoEstadoItem(
            formato=fmt_key,
            plantilla_key=fmt_info.get("plantilla_key", fmt_key.lower()),
            nombre=fmt_info["nombre"],
            descripcion=fmt_info["descripcion"],
            categoria=fmt_info["categoria"],
            es_requerido=fmt_info["es_requerido"],
            tiene_plantilla=tiene_plantilla,
            impacto=fmt_info["impacto"],
            upload_endpoint=fmt_info["upload_endpoint"],
            ultima_carga=ultima_carga,
            estado=estado,
            registros_ok=registros_ok,
            usuario_ultima_carga=usuario,
        ))

    return EstadoFormatosResponse(
        formatos=formatos,
        total=total,
        cargados_exitosos=cargados_exitosos,
        cargados_parcial=cargados_parcial,
        sin_cargar=sin_cargar,
        requeridos_faltantes=requeridos_faltantes,
    )


# ---------------------------------------------------------------------------
# Delete imported data for a specific format
# ---------------------------------------------------------------------------

#: Maps each format to the tables/models whose data should be deleted.
_FORMAT_TABLE_MAP: dict[str, list[type]] = {
    "FORMATO_1": [ProgramacionMensual, ProgramacionPresupuestal],
    "FORMATO_2": [ProgramacionPresupuestal],
    "FORMATO_3": [ProgramacionPresupuestal],
    "FORMATO_04": [ModificacionPresupuestal],
    "FORMATO_5A": [ProgramacionMensual],
    "FORMATO_5B": [ProgramacionMensual],
    "SIAF": [ProgramacionPresupuestal],
    "CUADRO_AO_META": [ActividadOperativa, MetaPresupuestal, UnidadEjecutora],
    "TABLAS": [ClasificadorGasto],
}


def limpiar_formato(db: Session, formato: str) -> dict[str, Any]:
    """Delete imported data and history records for a specific format.

    For formats that write to DB tables, truncates the relevant data.
    Always deletes the RegistroImportacion audit rows for that format.

    Returns a summary dict with deleted counts.
    """
    # Validate format exists in catalog
    valid_formats = {f["formato"] for f in _ESTADO_CATALOG}
    if formato not in valid_formats:
        raise ValueError(f"Formato '{formato}' no existe en el catalogo.")

    deleted_data = 0
    tables_affected: list[str] = []

    # Delete data from associated tables
    models = _FORMAT_TABLE_MAP.get(formato, [])
    for model in models:
        count = db.query(model).delete()
        deleted_data += count
        tables_affected.append(model.__tablename__)

    # Delete import history records for this format
    deleted_history = (
        db.query(RegistroImportacion)
        .filter(RegistroImportacion.formato == formato)
        .delete()
    )

    db.commit()

    logger.info(
        "limpiar_formato: formato='%s' data_deleted=%d history_deleted=%d tables=%s",
        formato, deleted_data, deleted_history, tables_affected,
    )

    return {
        "formato": formato,
        "registros_datos_eliminados": deleted_data,
        "registros_historial_eliminados": deleted_history,
        "tablas_afectadas": tables_affected,
    }
