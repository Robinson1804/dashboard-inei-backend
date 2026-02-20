"""
Alertas service layer.

All database access for the ``/api/alertas`` endpoints lives here.
The module provides two distinct responsibilities:

1. **Query helpers** — ``get_alertas``, ``get_resumen``, ``marcar_leida``,
   ``marcar_resuelta`` — standard CRUD operations on existing ``Alerta`` rows.

2. **Alert engine** — ``generar_alertas`` — evaluates all 8 business rules
   against the current database state for a given fiscal year and inserts
   new ``Alerta`` records for any threshold breaches discovered.

Alert rules (8 total)
---------------------
Rule 1  — Sub-ejecución AO < 70%              → ROJO
Rule 2  — Ejecución moderada AO 70-89%        → AMARILLO
Rule 3  — Sobre-ejecución AO > 110%           → ROJO
Rule 4  — Saldo presupuestal < 10% PIM        → AMARILLO
Rule 5  — Adquisición estancada > 30 días     → ROJO
Rule 6  — Contrato menor estancado > 15 días  → AMARILLO
Rule 7  — Fraccionamiento por cantidad        → ROJO
         (>= 3 contracts same UE/category/month)
Rule 8  — Fraccionamiento por monto           → ROJO
         (> 8 UIT = S/44,000 accumulated same category/quarter)

Design notes
------------
- The engine is idempotent per ``(tipo, entidad_id, entidad_tipo, anio)``
  tuple: it skips creating a duplicate alert if an unresolved one already
  exists for the same entity and rule.
- ``datetime.now(timezone.utc)`` is used for all timestamps.
- All eight rules are wrapped in individual try/except blocks so that one
  failing rule does not abort the entire generation run.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import extract, func
from sqlalchemy.orm import Session

from app.models.actividad_operativa import ActividadOperativa
from app.models.adquisicion import Adquisicion
from app.models.adquisicion_proceso import AdquisicionProceso
from app.models.alerta import Alerta
from app.models.contrato_menor import ContratoMenor
from app.models.contrato_menor_proceso import ContratoMenorProceso
from app.models.programacion_mensual import ProgramacionMensual
from app.models.programacion_presupuestal import ProgramacionPresupuestal
from app.models.unidad_ejecutora import UnidadEjecutora
from app.schemas.alerta import AlertaResumenResponse, AlertaResponse
from app.schemas.common import FilterParams
from app.utils.constants import (
    DIAS_PARALIZADO_ADQUISICION,
    DIAS_PARALIZADO_CONTRATO,
    FRACCIONAMIENTO_ACUMULADO_TRIMESTRE,
    FRACCIONAMIENTO_MAX_CONTRATOS_MES,
    SEMAFORO_AMARILLO_MIN,
    SEMAFORO_VERDE_MIN,
    UMBRAL_8_UIT,
)

logger = logging.getLogger(__name__)

# Sobre-ejecución threshold (>110% triggers ROJO)
_SOBRE_EJECUCION_MAX: float = 1.10

# Minimum PIM balance ratio before triggering a budget balance alert
_SALDO_MIN_RATIO: float = 0.10


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_pct(numerator: float, denominator: float) -> float:
    """Return numerator / denominator as a ratio (not percentage), or 0.0.

    Args:
        numerator: Dividend.
        denominator: Divisor.

    Returns:
        Ratio rounded to 4 decimal places; 0.0 when denominator is zero.
    """
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def _apply_alerta_filters(
    query: Any,
    filters: FilterParams,
    leida: bool | None,
    modulo: str | None,
) -> Any:
    """Apply filter constraints to an Alerta query.

    Args:
        query: Active SQLAlchemy query targeting ``Alerta``.
        filters: Year and UE constraints.
        leida: If not None, filter by read status.
        modulo: If not None, filter by module name.

    Returns:
        The query with applicable WHERE clauses appended.
    """
    if filters.ue_id is not None:
        query = query.filter(Alerta.ue_id == filters.ue_id)
    if leida is not None:
        query = query.filter(Alerta.leida.is_(leida))
    if modulo is not None:
        query = query.filter(Alerta.modulo == modulo)
    return query


def _alerta_exists(
    db: Session,
    tipo: str,
    entidad_id: int,
    entidad_tipo: str,
) -> bool:
    """Check if an unresolved alert of this type already exists for the entity.

    Used by the alert engine to achieve idempotency: prevents duplicate
    alerts being generated across multiple engine runs for the same breach.

    Args:
        db: Active SQLAlchemy session.
        tipo: Alert rule identifier.
        entidad_id: Source entity primary key.
        entidad_tipo: Source entity type name.

    Returns:
        ``True`` if an identical unresolved alert already exists.
    """
    return (
        db.query(Alerta.id)
        .filter(
            Alerta.tipo == tipo,
            Alerta.entidad_id == entidad_id,
            Alerta.entidad_tipo == entidad_tipo,
            Alerta.resuelta.is_(False),
        )
        .first()
    ) is not None


def _create_alerta(
    db: Session,
    tipo: str,
    nivel: str,
    titulo: str,
    descripcion: str,
    modulo: str,
    entidad_id: int,
    entidad_tipo: str,
    ue_id: int | None = None,
) -> Alerta:
    """Insert a new ``Alerta`` record and flush to assign a primary key.

    Does NOT commit — the caller is responsible for committing after all
    alerts for the current engine run have been inserted.

    Args:
        db: Active SQLAlchemy session.
        tipo: Alert rule type identifier.
        nivel: Severity — ``"ROJO"``, ``"AMARILLO"``, or ``"VERDE"``.
        titulo: Short human-readable title.
        descripcion: Detailed description.
        modulo: Dashboard module name.
        entidad_id: Source entity primary key.
        entidad_tipo: Source entity type name.
        ue_id: Optional FK to UnidadEjecutora.

    Returns:
        The newly flushed ``Alerta`` ORM instance.
    """
    alerta = Alerta(
        tipo=tipo,
        nivel=nivel,
        titulo=titulo,
        descripcion=descripcion,
        ue_id=ue_id,
        modulo=modulo,
        entidad_id=entidad_id,
        entidad_tipo=entidad_tipo,
        leida=False,
        resuelta=False,
        fecha_generacion=datetime.now(timezone.utc),
    )
    db.add(alerta)
    db.flush()
    return alerta


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------


def get_alertas(
    db: Session,
    filters: FilterParams,
    leida: bool | None = None,
    modulo: str | None = None,
) -> list[AlertaResponse]:
    """Return alerts matching the given filter criteria.

    Joins ``Alerta`` with ``UnidadEjecutora`` to resolve ``ue_sigla``,
    and orders results by ``fecha_generacion`` descending (newest first).

    Args:
        db: Active SQLAlchemy session.
        filters: UE constraint (``ue_id``; ``anio`` is not applicable here).
        leida: If provided, restrict to read (``True``) or unread (``False``) alerts.
        modulo: If provided, restrict to alerts from a specific module.

    Returns:
        List of ``AlertaResponse`` instances ordered newest first.
    """
    q = (
        db.query(
            Alerta,
            UnidadEjecutora.sigla.label("ue_sigla"),
        )
        .outerjoin(UnidadEjecutora, Alerta.ue_id == UnidadEjecutora.id)
    )
    q = _apply_alerta_filters(q, filters, leida, modulo)
    q = q.order_by(Alerta.fecha_generacion.desc())

    results: list[AlertaResponse] = []
    for alerta, ue_sigla in q.all():
        data = {
            "id": alerta.id,
            "tipo": alerta.tipo,
            "nivel": alerta.nivel,
            "titulo": alerta.titulo,
            "descripcion": alerta.descripcion,
            "ue_sigla": ue_sigla,
            "modulo": alerta.modulo,
            "entidad_id": alerta.entidad_id,
            "entidad_tipo": alerta.entidad_tipo,
            "leida": alerta.leida,
            "resuelta": alerta.resuelta,
            "fecha_generacion": alerta.fecha_generacion,
        }
        results.append(AlertaResponse(**data))

    logger.debug("get_alertas: %d alerts returned", len(results))
    return results


def get_resumen(db: Session, filters: FilterParams) -> AlertaResumenResponse:
    """Return aggregate alert counts for the dashboard notification summary.

    Computes total, unread, ROJO, AMARILLO counts and a per-module breakdown
    using a single aggregation query.

    Args:
        db: Active SQLAlchemy session.
        filters: UE constraint.

    Returns:
        An ``AlertaResumenResponse`` with all count fields populated.
    """
    q_base = db.query(Alerta)
    if filters.ue_id is not None:
        q_base = q_base.filter(Alerta.ue_id == filters.ue_id)

    total: int = q_base.count()
    no_leidas: int = q_base.filter(Alerta.leida.is_(False)).count()
    rojas: int = q_base.filter(Alerta.nivel == "ROJO").count()
    amarillas: int = q_base.filter(Alerta.nivel == "AMARILLO").count()

    # Per-module breakdown
    modulo_rows = (
        db.query(
            Alerta.modulo.label("modulo"),
            func.count(Alerta.id).label("cnt"),
        )
        .group_by(Alerta.modulo)
        .all()
    )
    by_modulo: dict[str, int] = {
        row.modulo: row.cnt for row in modulo_rows if row.modulo is not None
    }

    logger.debug(
        "get_resumen alertas: total=%d no_leidas=%d rojas=%d amarillas=%d",
        total, no_leidas, rojas, amarillas,
    )

    return AlertaResumenResponse(
        total=total,
        no_leidas=no_leidas,
        rojas=rojas,
        amarillas=amarillas,
        by_modulo=by_modulo,
    )


def marcar_leida(db: Session, alerta_id: int) -> Alerta:
    """Mark a single alert as read and persist the read timestamp.

    Args:
        db: Active SQLAlchemy session.
        alerta_id: Primary key of the alert to mark.

    Returns:
        The updated ``Alerta`` ORM instance.

    Raises:
        HTTPException 404: If no alert with ``alerta_id`` exists.
    """
    alerta: Alerta | None = db.query(Alerta).filter(Alerta.id == alerta_id).first()
    if alerta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alerta con id={alerta_id} no encontrada.",
        )
    alerta.leida = True
    alerta.fecha_lectura = datetime.now(timezone.utc)
    db.commit()
    db.refresh(alerta)
    logger.debug("marcar_leida: alerta id=%d marcada como leída", alerta_id)
    return alerta


def marcar_resuelta(db: Session, alerta_id: int) -> Alerta:
    """Mark a single alert as resolved and persist the resolution timestamp.

    Args:
        db: Active SQLAlchemy session.
        alerta_id: Primary key of the alert to resolve.

    Returns:
        The updated ``Alerta`` ORM instance.

    Raises:
        HTTPException 404: If no alert with ``alerta_id`` exists.
    """
    alerta: Alerta | None = db.query(Alerta).filter(Alerta.id == alerta_id).first()
    if alerta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alerta con id={alerta_id} no encontrada.",
        )
    alerta.resuelta = True
    alerta.fecha_resolucion = datetime.now(timezone.utc)
    db.commit()
    db.refresh(alerta)
    logger.debug("marcar_resuelta: alerta id=%d marcada como resuelta", alerta_id)
    return alerta


# ---------------------------------------------------------------------------
# Alert engine — generar_alertas
# ---------------------------------------------------------------------------


def generar_alertas(db: Session, anio: int) -> int:
    """Run all 8 alert rules for a fiscal year and insert new Alerta records.

    This is the main alert engine.  It evaluates each of the 8 defined
    business rules in sequence, creates ``Alerta`` records for any threshold
    breaches found, and returns the total count of new alerts inserted.

    Idempotency is enforced: a rule will not create a duplicate ``Alerta``
    if an unresolved alert of the same ``tipo`` for the same entity already
    exists in the database.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate (e.g. 2026).

    Returns:
        Total number of new ``Alerta`` records inserted in this run.
    """
    count = 0

    # -----------------------------------------------------------------------
    # Rule 1 — Sub-ejecución AO < 70% → ROJO
    # Rule 2 — Ejecución moderada AO 70–89% → AMARILLO
    # Rule 3 — Sobre-ejecución AO > 110% → ROJO
    # -----------------------------------------------------------------------
    try:
        count += _rule_ao_ejecucion(db, anio)
    except Exception:
        logger.exception("generar_alertas: error evaluating AO execution rules")

    # -----------------------------------------------------------------------
    # Rule 4 — Saldo presupuestal < 10% PIM → AMARILLO
    # -----------------------------------------------------------------------
    try:
        count += _rule_saldo_presupuestal(db, anio)
    except Exception:
        logger.exception("generar_alertas: error evaluating budget balance rule")

    # -----------------------------------------------------------------------
    # Rule 5 — Adquisición estancada > 30 días → ROJO
    # -----------------------------------------------------------------------
    try:
        count += _rule_adquisicion_estancada(db, anio)
    except Exception:
        logger.exception("generar_alertas: error evaluating stalled acquisition rule")

    # -----------------------------------------------------------------------
    # Rule 6 — Contrato menor estancado > 15 días → AMARILLO
    # -----------------------------------------------------------------------
    try:
        count += _rule_contrato_estancado(db, anio)
    except Exception:
        logger.exception("generar_alertas: error evaluating stalled minor contract rule")

    # -----------------------------------------------------------------------
    # Rule 7 — Fraccionamiento por cantidad → ROJO
    # -----------------------------------------------------------------------
    try:
        count += _rule_fraccionamiento_cantidad(db, anio)
    except Exception:
        logger.exception("generar_alertas: error evaluating quantity-fractionation rule")

    # -----------------------------------------------------------------------
    # Rule 8 — Fraccionamiento por monto → ROJO
    # -----------------------------------------------------------------------
    try:
        count += _rule_fraccionamiento_monto(db, anio)
    except Exception:
        logger.exception("generar_alertas: error evaluating amount-fractionation rule")

    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("generar_alertas: commit failed — rolling back")
        raise

    logger.info(
        "generar_alertas: %d new alerts generated for anio=%d", count, anio
    )
    return count


# ---------------------------------------------------------------------------
# Rule implementations (private)
# ---------------------------------------------------------------------------


def _rule_ao_ejecucion(db: Session, anio: int) -> int:
    """Rules 1, 2, 3 — AO execution level semaphore.

    Aggregates programado and ejecutado from programacion_mensual for each
    AO in the given year, computes the execution ratio, and classifies:
    - < 70%   → Rule 1 (ROJO — sub-ejecución)
    - 70–89%  → Rule 2 (AMARILLO — ejecución moderada)
    - > 110%  → Rule 3 (ROJO — sobre-ejecución)

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate.

    Returns:
        Number of new alerts inserted.
    """
    agg_rows = (
        db.query(
            ActividadOperativa.id.label("ao_id"),
            ActividadOperativa.nombre.label("ao_nombre"),
            ActividadOperativa.ue_id.label("ue_id"),
            UnidadEjecutora.sigla.label("ue_sigla"),
            func.coalesce(func.sum(ProgramacionMensual.programado), 0).label("prog"),
            func.coalesce(func.sum(ProgramacionMensual.ejecutado), 0).label("ejec"),
        )
        .join(UnidadEjecutora, ActividadOperativa.ue_id == UnidadEjecutora.id)
        .join(
            ProgramacionPresupuestal,
            (ProgramacionPresupuestal.meta_id == ActividadOperativa.meta_id)
            & (ProgramacionPresupuestal.ue_id == ActividadOperativa.ue_id),
        )
        .join(
            ProgramacionMensual,
            ProgramacionMensual.programacion_presupuestal_id == ProgramacionPresupuestal.id,
        )
        .filter(ActividadOperativa.anio == anio, ActividadOperativa.activo.is_(True))
        .group_by(
            ActividadOperativa.id,
            ActividadOperativa.nombre,
            ActividadOperativa.ue_id,
            UnidadEjecutora.sigla,
        )
        .all()
    )

    count = 0
    for row in agg_rows:
        prog = float(row.prog)
        ejec = float(row.ejec)
        ratio = _safe_pct(ejec, prog)

        if ratio < SEMAFORO_AMARILLO_MIN:
            tipo = "SUB_EJECUCION_AO"
            nivel = "ROJO"
            titulo = f"Sub-ejecución crítica: {row.ao_nombre[:60]}"
            descripcion = (
                f"La actividad operativa '{row.ao_nombre}' ({row.ue_sigla}) "
                f"presenta una ejecución del {ratio * 100:.1f}%, por debajo del "
                f"umbral mínimo del {SEMAFORO_AMARILLO_MIN * 100:.0f}%."
            )
        elif ratio < SEMAFORO_VERDE_MIN:
            tipo = "EJECUCION_MODERADA_AO"
            nivel = "AMARILLO"
            titulo = f"Ejecución moderada: {row.ao_nombre[:60]}"
            descripcion = (
                f"La actividad operativa '{row.ao_nombre}' ({row.ue_sigla}) "
                f"presenta una ejecución del {ratio * 100:.1f}%, en la zona de alerta "
                f"({SEMAFORO_AMARILLO_MIN * 100:.0f}%–{SEMAFORO_VERDE_MIN * 100:.0f}%)."
            )
        elif ratio > _SOBRE_EJECUCION_MAX:
            tipo = "SOBRE_EJECUCION_AO"
            nivel = "ROJO"
            titulo = f"Sobre-ejecución: {row.ao_nombre[:60]}"
            descripcion = (
                f"La actividad operativa '{row.ao_nombre}' ({row.ue_sigla}) "
                f"presenta una sobre-ejecución del {ratio * 100:.1f}% "
                f"(supera el {_SOBRE_EJECUCION_MAX * 100:.0f}% del programado)."
            )
        else:
            continue  # within green range — no alert needed

        if _alerta_exists(db, tipo, row.ao_id, "actividad_operativa"):
            continue

        _create_alerta(
            db,
            tipo=tipo,
            nivel=nivel,
            titulo=titulo,
            descripcion=descripcion,
            modulo="ACTIVIDADES_OPERATIVAS",
            entidad_id=row.ao_id,
            entidad_tipo="actividad_operativa",
            ue_id=row.ue_id,
        )
        count += 1

    logger.debug("_rule_ao_ejecucion: %d alerts created", count)
    return count


def _rule_saldo_presupuestal(db: Session, anio: int) -> int:
    """Rule 4 — Budget balance below 10% of PIM.

    Groups ``ProgramacionPresupuestal`` by UE for the given year and creates
    an AMARILLO alert for any UE where (saldo / pim) < 10%.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate.

    Returns:
        Number of new alerts inserted.
    """
    agg_rows = (
        db.query(
            ProgramacionPresupuestal.ue_id.label("ue_id"),
            UnidadEjecutora.sigla.label("ue_sigla"),
            func.coalesce(func.sum(ProgramacionPresupuestal.pim), 0).label("pim_total"),
            func.coalesce(func.sum(ProgramacionPresupuestal.saldo), 0).label("saldo_total"),
        )
        .join(UnidadEjecutora, ProgramacionPresupuestal.ue_id == UnidadEjecutora.id)
        .filter(ProgramacionPresupuestal.anio == anio)
        .group_by(ProgramacionPresupuestal.ue_id, UnidadEjecutora.sigla)
        .all()
    )

    count = 0
    for row in agg_rows:
        pim = float(row.pim_total)
        saldo = float(row.saldo_total)
        ratio = _safe_pct(saldo, pim)

        if pim <= 0 or ratio >= _SALDO_MIN_RATIO:
            continue

        tipo = "SALDO_BAJO_PRESUPUESTO"
        if _alerta_exists(db, tipo, row.ue_id, "unidad_ejecutora"):
            continue

        _create_alerta(
            db,
            tipo=tipo,
            nivel="AMARILLO",
            titulo=f"Saldo presupuestal bajo: {row.ue_sigla}",
            descripcion=(
                f"La unidad ejecutora '{row.ue_sigla}' tiene un saldo disponible "
                f"de S/ {saldo:,.2f} ({ratio * 100:.1f}% del PIM), por debajo del "
                f"umbral de alerta del {_SALDO_MIN_RATIO * 100:.0f}%."
            ),
            modulo="PRESUPUESTO",
            entidad_id=row.ue_id,
            entidad_tipo="unidad_ejecutora",
            ue_id=row.ue_id,
        )
        count += 1

    logger.debug("_rule_saldo_presupuestal: %d alerts created", count)
    return count


def _rule_adquisicion_estancada(db: Session, anio: int) -> int:
    """Rule 5 — Acquisition stalled for more than 30 days without progress.

    Identifies active acquisitions (not yet CULMINADO/DESIERTO/NULO) where
    the most recent ``AdquisicionProceso.fecha_real_fin`` is older than
    ``DIAS_PARALIZADO_ADQUISICION`` days, or falls back to ``Adquisicion.updated_at``
    when no process milestone has a real end date recorded.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate.

    Returns:
        Number of new alerts inserted.
    """
    terminal_states = {"CULMINADO", "DESIERTO", "NULO"}
    cutoff = datetime.now(timezone.utc) - timedelta(days=DIAS_PARALIZADO_ADQUISICION)

    active_adqs = (
        db.query(Adquisicion)
        .filter(
            Adquisicion.anio == anio,
            Adquisicion.estado.notin_(terminal_states),
        )
        .all()
    )

    count = 0
    for adq in active_adqs:
        # Find the most recent completed-process date for this acquisition
        last_proceso = (
            db.query(func.max(AdquisicionProceso.fecha_real_fin))
            .filter(
                AdquisicionProceso.adquisicion_id == adq.id,
                AdquisicionProceso.fecha_real_fin.isnot(None),
            )
            .scalar()
        )

        if last_proceso is not None:
            # Convert date → datetime for comparison (date has no tzinfo)
            last_activity = datetime(
                last_proceso.year, last_proceso.month, last_proceso.day,
                tzinfo=timezone.utc,
            )
        else:
            # Fall back to the acquisition's own updated_at timestamp
            updated = adq.updated_at
            if updated is None:
                continue
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            last_activity = updated

        if last_activity > cutoff:
            continue  # activity within the threshold — no alert needed

        tipo = "ADQUISICION_ESTANCADA"
        if _alerta_exists(db, tipo, adq.id, "adquisicion"):
            continue

        dias_sin_actividad = (datetime.now(timezone.utc) - last_activity).days
        _create_alerta(
            db,
            tipo=tipo,
            nivel="ROJO",
            titulo=f"Adquisición estancada: {adq.codigo}",
            descripcion=(
                f"La adquisición '{adq.codigo}' ({adq.descripcion[:80]}) "
                f"lleva {dias_sin_actividad} días sin registrar avance. "
                f"Estado actual: {adq.estado}."
            ),
            modulo="ADQUISICIONES",
            entidad_id=adq.id,
            entidad_tipo="adquisicion",
            ue_id=adq.ue_id,
        )
        count += 1

    logger.debug("_rule_adquisicion_estancada: %d alerts created", count)
    return count


def _rule_contrato_estancado(db: Session, anio: int) -> int:
    """Rule 6 — Minor contract stalled for more than 15 days.

    Same logic as Rule 5 but applied to ``ContratoMenor`` / ``ContratoMenorProceso``
    with the shorter DIAS_PARALIZADO_CONTRATO threshold.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate.

    Returns:
        Number of new alerts inserted.
    """
    terminal_states = {"EJECUTADO", "PAGADO"}
    cutoff = datetime.now(timezone.utc) - timedelta(days=DIAS_PARALIZADO_CONTRATO)

    active_contratos = (
        db.query(ContratoMenor)
        .filter(
            ContratoMenor.anio == anio,
            ContratoMenor.estado.notin_(terminal_states),
        )
        .all()
    )

    count = 0
    for cm in active_contratos:
        last_proceso = (
            db.query(func.max(ContratoMenorProceso.fecha_fin))
            .filter(
                ContratoMenorProceso.contrato_menor_id == cm.id,
                ContratoMenorProceso.fecha_fin.isnot(None),
                ContratoMenorProceso.estado == "COMPLETADO",
            )
            .scalar()
        )

        if last_proceso is not None:
            last_activity = datetime(
                last_proceso.year, last_proceso.month, last_proceso.day,
                tzinfo=timezone.utc,
            )
        else:
            updated = cm.updated_at
            if updated is None:
                continue
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            last_activity = updated

        if last_activity > cutoff:
            continue

        tipo = "CONTRATO_MENOR_ESTANCADO"
        if _alerta_exists(db, tipo, cm.id, "contrato_menor"):
            continue

        dias_sin_actividad = (datetime.now(timezone.utc) - last_activity).days
        codigo = cm.codigo or f"CM-ID-{cm.id}"
        descripcion_cm = (cm.descripcion or "Sin descripción")[:80]
        _create_alerta(
            db,
            tipo=tipo,
            nivel="AMARILLO",
            titulo=f"Contrato menor estancado: {codigo}",
            descripcion=(
                f"El contrato menor '{codigo}' ({descripcion_cm}) "
                f"lleva {dias_sin_actividad} días sin registrar avance. "
                f"Estado actual: {cm.estado}."
            ),
            modulo="CONTRATOS_MENORES",
            entidad_id=cm.id,
            entidad_tipo="contrato_menor",
            ue_id=cm.ue_id,
        )
        count += 1

    logger.debug("_rule_contrato_estancado: %d alerts created", count)
    return count


def _rule_fraccionamiento_cantidad(db: Session, anio: int) -> int:
    """Rule 7 — Quantity-based fractionation detection.

    Detects when the same UE has issued >= ``FRACCIONAMIENTO_MAX_CONTRATOS_MES``
    (3+) minor contracts in the same category within a single calendar month,
    which may indicate deliberate splitting to avoid the 8-UIT threshold.

    Groups by ``(ue_id, categoria, month(created_at))``.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate.

    Returns:
        Number of new alerts inserted.
    """
    agg_rows = (
        db.query(
            ContratoMenor.ue_id.label("ue_id"),
            UnidadEjecutora.sigla.label("ue_sigla"),
            ContratoMenor.categoria.label("categoria"),
            extract("month", ContratoMenor.created_at).label("mes"),
            func.count(ContratoMenor.id).label("cnt"),
        )
        .join(UnidadEjecutora, ContratoMenor.ue_id == UnidadEjecutora.id)
        .filter(ContratoMenor.anio == anio, ContratoMenor.categoria.isnot(None))
        .group_by(
            ContratoMenor.ue_id,
            UnidadEjecutora.sigla,
            ContratoMenor.categoria,
            extract("month", ContratoMenor.created_at),
        )
        .having(func.count(ContratoMenor.id) >= FRACCIONAMIENTO_MAX_CONTRATOS_MES)
        .all()
    )

    count = 0
    for row in agg_rows:
        tipo = "FRACCIONAMIENTO_CANTIDAD"
        # Use ue_id as pseudo entity id since this is a group-level alert
        entity_id = int(row.ue_id * 1000 + int(row.mes))  # composite pseudo-key
        if _alerta_exists(db, tipo, entity_id, "contrato_menor_grupo"):
            continue

        _create_alerta(
            db,
            tipo=tipo,
            nivel="ROJO",
            titulo=f"Posible fraccionamiento (cantidad): {row.ue_sigla}",
            descripcion=(
                f"La unidad ejecutora '{row.ue_sigla}' registró {int(row.cnt)} "
                f"contratos menores de categoría '{row.categoria}' en el mes "
                f"{int(row.mes)} del año {anio}. Esto puede constituir "
                f"fraccionamiento (umbral: {FRACCIONAMIENTO_MAX_CONTRATOS_MES} contratos/mes)."
            ),
            modulo="CONTRATOS_MENORES",
            entidad_id=entity_id,
            entidad_tipo="contrato_menor_grupo",
            ue_id=row.ue_id,
        )
        count += 1

    logger.debug("_rule_fraccionamiento_cantidad: %d alerts created", count)
    return count


def _rule_fraccionamiento_monto(db: Session, anio: int) -> int:
    """Rule 8 — Amount-based fractionation detection.

    Detects when the cumulative ``monto_ejecutado`` of minor contracts for
    the same UE and category within a trimester exceeds
    ``FRACCIONAMIENTO_ACUMULADO_TRIMESTRE`` (S/ 44,000 = 8 UIT).

    Groups by ``(ue_id, categoria, trimester)``.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to evaluate.

    Returns:
        Number of new alerts inserted.
    """
    # Trimester derived as CEIL(month / 3)
    trimester_expr = func.ceil(extract("month", ContratoMenor.created_at) / 3)

    agg_rows = (
        db.query(
            ContratoMenor.ue_id.label("ue_id"),
            UnidadEjecutora.sigla.label("ue_sigla"),
            ContratoMenor.categoria.label("categoria"),
            trimester_expr.label("trimestre"),
            func.coalesce(
                func.sum(ContratoMenor.monto_ejecutado), 0
            ).label("monto_total"),
        )
        .join(UnidadEjecutora, ContratoMenor.ue_id == UnidadEjecutora.id)
        .filter(
            ContratoMenor.anio == anio,
            ContratoMenor.categoria.isnot(None),
            ContratoMenor.monto_ejecutado.isnot(None),
        )
        .group_by(
            ContratoMenor.ue_id,
            UnidadEjecutora.sigla,
            ContratoMenor.categoria,
            trimester_expr,
        )
        .having(
            func.sum(ContratoMenor.monto_ejecutado) > FRACCIONAMIENTO_ACUMULADO_TRIMESTRE
        )
        .all()
    )

    count = 0
    for row in agg_rows:
        tipo = "FRACCIONAMIENTO_MONTO"
        entity_id = int(row.ue_id * 10000 + int(row.trimestre))
        if _alerta_exists(db, tipo, entity_id, "contrato_menor_grupo"):
            continue

        monto = float(row.monto_total)
        _create_alerta(
            db,
            tipo=tipo,
            nivel="ROJO",
            titulo=f"Posible fraccionamiento (monto): {row.ue_sigla}",
            descripcion=(
                f"La unidad ejecutora '{row.ue_sigla}' acumuló S/ {monto:,.2f} "
                f"en contratos menores de categoría '{row.categoria}' durante el "
                f"trimestre {int(row.trimestre)} del año {anio}. Esto supera el "
                f"umbral de S/ {FRACCIONAMIENTO_ACUMULADO_TRIMESTRE:,} (8 UIT = S/ {UMBRAL_8_UIT:,})."
            ),
            modulo="CONTRATOS_MENORES",
            entidad_id=entity_id,
            entidad_tipo="contrato_menor_grupo",
            ue_id=row.ue_id,
        )
        count += 1

    logger.debug("_rule_fraccionamiento_monto: %d alerts created", count)
    return count
