"""
Contratos Menores ≤8 UIT service layer.

All database access for the ``/api/contratos-menores`` endpoints lives here.
Functions receive a SQLAlchemy ``Session`` and produce schema instances or ORM
objects ready for serialisation by FastAPI.

Design notes
------------
- ``func.coalesce(..., 0)`` guards every aggregate against NULL on empty sets.
- Execution percentage and derived values are computed in Python after DB
  aggregation to keep SQL portable across PostgreSQL and SQLite (test env).
- The auto-generated contract code uses a zero-padded sequence so that codes
  sort lexicographically: ``CM-2026-001``, ``CM-2026-002``, ...
- Fraccionamiento detection runs two independent GROUP BY queries so each
  rule can fire independently and produce its own ``FraccionamientoAlerta``.
- Spanish month abbreviations and quarter labels are built purely in Python so
  the API output does not depend on the database locale setting.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.contrato_menor import ContratoMenor
from app.models.contrato_menor_proceso import ContratoMenorProceso
from app.models.meta_presupuestal import MetaPresupuestal
from app.models.proveedor import Proveedor
from app.models.unidad_ejecutora import UnidadEjecutora
from app.schemas.common import FilterParams, PaginationParams
from app.schemas.contrato_menor import (
    ContratoMenorCreate,
    ContratoMenorProcesoCreate,
    ContratoMenorProcesoResponse,
    ContratoMenorProcesoUpdate,
    ContratoMenorResponse,
    ContratoMenorUpdate,
    FraccionamientoAlerta,
    GraficoContratoMenorItem,
    KpiContratosMenoresResponse,
    TablaContratosMenoresResponse,
)
from app.utils.constants import (
    FRACCIONAMIENTO_ACUMULADO_TRIMESTRE,
    FRACCIONAMIENTO_MAX_CONTRATOS_MES,
    UMBRAL_8_UIT,
)
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

_MES_LABELS: list[str] = [
    "",                                      # index 0 — unused
    "Ene", "Feb", "Mar", "Abr", "May", "Jun",
    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
]

# States that count as "completed" for KPI calculations
_ESTADOS_COMPLETADO: frozenset[str] = frozenset({"EJECUTADO", "PAGADO"})
# States that count as "in progress"
_ESTADOS_EN_PROCESO: frozenset[str] = frozenset({"EN_PROCESO", "ORDEN_EMITIDA"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_cm_filters(query: Any, filters: FilterParams) -> Any:
    """Apply standard FilterParams constraints to a ContratoMenor query.

    Supports ``anio``, ``ue_id``, and ``meta_id`` fields.
    ``fuente_financiamiento`` is not a column on ContratoMenor and is ignored.

    Args:
        query: An active SQLAlchemy query object targeting ``ContratoMenor``
               (possibly already joined).
        filters: Domain filter parameters from the HTTP request.

    Returns:
        The query with WHERE clauses appended for each non-None filter field.
    """
    if filters.anio is not None:
        query = query.filter(ContratoMenor.anio == filters.anio)
    if filters.ue_id is not None:
        query = query.filter(ContratoMenor.ue_id == filters.ue_id)
    if filters.meta_id is not None:
        query = query.filter(ContratoMenor.meta_id == filters.meta_id)
    return query


def _safe_pct(numerator: float, denominator: float) -> float:
    """Return numerator / denominator × 100, capped at 100.0; 0.0 if denom is zero.

    Args:
        numerator: The dividend value.
        denominator: The divisor value.

    Returns:
        Percentage rounded to two decimal places (0.0–100.0).
    """
    if denominator == 0:
        return 0.0
    return round(min((numerator / denominator) * 100, 100.0), 2)


def _build_response(row: ContratoMenor) -> ContratoMenorResponse:
    """Construct a ``ContratoMenorResponse`` from a ``ContratoMenor`` ORM object.

    Resolves denormalised string fields by traversing lazy-loaded relationships
    (``unidad_ejecutora``, ``meta_presupuestal``, ``proveedor``, ``procesos``).

    Args:
        row: A ``ContratoMenor`` instance loaded from the database.

    Returns:
        A fully-populated ``ContratoMenorResponse``.
    """
    procesos: list[ContratoMenorProcesoResponse] = [
        ContratoMenorProcesoResponse.model_validate(p)
        for p in (row.procesos or [])
    ]

    return ContratoMenorResponse(
        id=row.id,
        codigo=row.codigo,
        anio=row.anio,
        descripcion=row.descripcion,
        tipo_objeto=row.tipo_objeto,
        categoria=row.categoria,
        estado=row.estado,
        monto_estimado=float(row.monto_estimado) if row.monto_estimado is not None else None,
        monto_ejecutado=float(row.monto_ejecutado) if row.monto_ejecutado is not None else None,
        n_orden=row.n_orden,
        n_cotizaciones=row.n_cotizaciones,
        ue_id=row.ue_id,
        ue_sigla=(
            row.unidad_ejecutora.sigla
            if row.unidad_ejecutora is not None
            else None
        ),
        meta_id=row.meta_id,
        meta_codigo=(
            row.meta_presupuestal.codigo
            if row.meta_presupuestal is not None
            else None
        ),
        proveedor_id=row.proveedor_id,
        proveedor_razon_social=(
            row.proveedor.razon_social
            if row.proveedor is not None
            else None
        ),
        created_at=row.created_at,
        updated_at=row.updated_at,
        procesos=procesos,
    )


def _next_sequence(db: Session, anio: int) -> int:
    """Return the next available sequential number for a given fiscal year.

    Scans all ``ContratoMenor.codigo`` values that match ``"CM-{anio}-"`` and
    returns ``max_found + 1``.  Returns ``1`` if no contracts exist yet.

    Args:
        db: Active SQLAlchemy session.
        anio: The fiscal year for the new sequence slot.

    Returns:
        Next integer sequence number (1-based).
    """
    prefix = f"CM-{anio}-"
    rows = (
        db.query(ContratoMenor.codigo)
        .filter(ContratoMenor.codigo.like(f"{prefix}%"))
        .all()
    )
    if not rows:
        return 1

    max_seq = 0
    for (codigo,) in rows:
        if codigo and len(codigo) > len(prefix):
            tail = codigo[len(prefix):]
            if tail.isdigit():
                max_seq = max(max_seq, int(tail))
    return max_seq + 1


# ---------------------------------------------------------------------------
# Public service functions — read operations
# ---------------------------------------------------------------------------


def get_kpis(db: Session, filters: FilterParams) -> KpiContratosMenoresResponse:
    """Aggregate top-level KPI figures for the Contratos Menores dashboard.

    Computes total contract count, monto sum, completed and in-progress counts,
    overall completion percentage, and the number of active fraccionamiento alerts.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, and meta constraints.

    Returns:
        A ``KpiContratosMenoresResponse`` with all six KPI values.
    """
    q = db.query(ContratoMenor)
    q = _apply_cm_filters(q, filters)
    all_rows = q.all()

    total = len(all_rows)
    monto_total = sum(
        float(r.monto_estimado) for r in all_rows if r.monto_estimado is not None
    )
    completados = sum(1 for r in all_rows if r.estado in _ESTADOS_COMPLETADO)
    en_proceso = sum(1 for r in all_rows if r.estado in _ESTADOS_EN_PROCESO)
    porcentaje_avance = _safe_pct(completados, total)

    # Count fraccionamiento alerts for the filtered year/UE
    anio = filters.anio or datetime.date.today().year
    alertas = detect_fraccionamiento(db, anio=anio, ue_id=filters.ue_id)
    alerta_fraccionamiento = len(alertas)

    logger.debug(
        "get_kpis: total=%d monto=%.2f completados=%d alertas=%d",
        total, monto_total, completados, alerta_fraccionamiento,
    )

    return KpiContratosMenoresResponse(
        total=total,
        monto_total=round(monto_total, 2),
        completados=completados,
        en_proceso=en_proceso,
        porcentaje_avance=porcentaje_avance,
        alerta_fraccionamiento=alerta_fraccionamiento,
    )


def get_graficos(
    db: Session, filters: FilterParams
) -> list[GraficoContratoMenorItem]:
    """Return distribution data for minor-contracts charts.

    Produces two blocks of ``GraficoContratoMenorItem`` concatenated:
    1. Distribution by ``estado`` — for the status breakdown chart.
    2. Distribution by ``tipo_objeto`` — for the object-type chart.

    Each block is ordered by ``cantidad`` descending.  A ``"group"`` key is
    NOT added to the schema to keep it simple; the router can split the list
    by index if needed, or the frontend can group by ``label`` prefix.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, and meta constraints.

    Returns:
        Combined list of ``GraficoContratoMenorItem`` — first the estado
        breakdown, then the tipo_objeto breakdown.
    """

    def _build_items(
        grouping_col: Any,
    ) -> list[GraficoContratoMenorItem]:
        """Aggregate contracts by *grouping_col* and build chart items."""
        q = (
            db.query(
                grouping_col.label("label"),
                func.count(ContratoMenor.id).label("cantidad"),
                func.coalesce(func.sum(ContratoMenor.monto_estimado), 0).label("monto"),
            )
        )
        q = _apply_cm_filters(q, filters)
        q = q.group_by(grouping_col).order_by(func.count(ContratoMenor.id).desc())
        rows = q.all()

        total_count = sum(r.cantidad for r in rows)
        items: list[GraficoContratoMenorItem] = []
        for row in rows:
            items.append(
                GraficoContratoMenorItem(
                    label=row.label or "SIN_ESTADO",
                    cantidad=row.cantidad,
                    monto=round(float(row.monto), 2),
                    porcentaje=_safe_pct(row.cantidad, total_count),
                )
            )
        return items

    estado_items = _build_items(ContratoMenor.estado)
    tipo_items = _build_items(ContratoMenor.tipo_objeto)
    result = estado_items + tipo_items

    logger.debug(
        "get_graficos: %d estado items, %d tipo_objeto items",
        len(estado_items), len(tipo_items),
    )
    return result


def get_tabla(
    db: Session,
    filters: FilterParams,
    pagination: PaginationParams,
) -> TablaContratosMenoresResponse:
    """Return a paginated, fully-joined table of minor contracts.

    Joins ``ContratoMenor`` → ``UnidadEjecutora``, ``MetaPresupuestal``, and
    ``Proveedor`` to resolve human-readable labels for every row.  Eager-load
    of ``procesos`` is skipped in the table view to keep queries fast; the
    detail endpoint loads them.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, and meta constraints.
        pagination: Page number and page size.

    Returns:
        A ``TablaContratosMenoresResponse`` with the current page of rows,
        total row count, and pagination metadata.
    """
    base_q = (
        db.query(ContratoMenor)
        .outerjoin(UnidadEjecutora, ContratoMenor.ue_id == UnidadEjecutora.id)
        .outerjoin(MetaPresupuestal, ContratoMenor.meta_id == MetaPresupuestal.id)
        .outerjoin(Proveedor, ContratoMenor.proveedor_id == Proveedor.id)
    )
    base_q = _apply_cm_filters(base_q, filters)
    base_q = base_q.order_by(ContratoMenor.id.desc())

    total: int = base_q.count()
    offset = (pagination.page - 1) * pagination.page_size
    page_rows = base_q.offset(offset).limit(pagination.page_size).all()

    rows: list[ContratoMenorResponse] = [_build_response(r) for r in page_rows]

    logger.debug(
        "get_tabla: page=%d size=%d total=%d returned=%d",
        pagination.page, pagination.page_size, total, len(rows),
    )

    return TablaContratosMenoresResponse(
        rows=rows,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )


def get_detalle(db: Session, contrato_id: int) -> ContratoMenorResponse:
    """Return the full detail of a single minor contract including its procesos.

    Args:
        db: Active SQLAlchemy session.
        contrato_id: Primary key of the contract to retrieve.

    Returns:
        A ``ContratoMenorResponse`` with the complete stepper timeline.

    Raises:
        HTTPException 404: If no contract with the given ID exists.
    """
    row: ContratoMenor | None = (
        db.query(ContratoMenor).filter(ContratoMenor.id == contrato_id).first()
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contrato menor con ID {contrato_id} no encontrado.",
        )

    logger.debug("get_detalle: contrato_id=%d codigo=%s", contrato_id, row.codigo)
    return _build_response(row)


# ---------------------------------------------------------------------------
# Public service functions — fraccionamiento detection
# ---------------------------------------------------------------------------


def detect_fraccionamiento(
    db: Session,
    anio: int,
    ue_id: int | None = None,
) -> list[FraccionamientoAlerta]:
    """Detect fraccionamiento patterns in minor contracting for the given year.

    Applies two independent rules and merges the resulting alert lists:

    **Rule 1 — CANTIDAD** (Art. 49.2 Ley 32069):
        Three or more contracts from the same DDNNTT (``ue_id``) in the same
        ``categoria`` created in the **same calendar month** → alert type
        ``"CANTIDAD"``.

    **Rule 2 — MONTO** (Art. 49.2 Ley 32069):
        Accumulated ``monto_estimado`` for the same DDNNTT + ``categoria``
        combination in the **same calendar quarter** exceeds
        ``UMBRAL_8_UIT`` (S/44,000) → alert type ``"MONTO"``.

    Both rules filter to ``contrato_menor.anio == anio`` and, if ``ue_id`` is
    supplied, further restrict to that unit.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year to inspect.
        ue_id: Optional — restrict detection to a single DDNNTT.

    Returns:
        Deduplicated list of ``FraccionamientoAlerta`` instances, Rule 1
        results first, then Rule 2 results.
    """
    alertas: list[FraccionamientoAlerta] = []

    # -----------------------------------------------------------------------
    # Rule 1 — CANTIDAD: >= 3 contracts same UE + categoria per calendar month
    # -----------------------------------------------------------------------
    q1 = (
        db.query(
            UnidadEjecutora.sigla.label("ue_sigla"),
            ContratoMenor.categoria.label("categoria"),
            func.extract("month", ContratoMenor.created_at).label("mes_num"),
            func.count(ContratoMenor.id).label("cantidad"),
            func.coalesce(func.sum(ContratoMenor.monto_estimado), 0).label("monto"),
        )
        .join(UnidadEjecutora, ContratoMenor.ue_id == UnidadEjecutora.id)
        .filter(ContratoMenor.anio == anio)
    )
    if ue_id is not None:
        q1 = q1.filter(ContratoMenor.ue_id == ue_id)

    q1 = (
        q1.group_by(
            UnidadEjecutora.id,
            UnidadEjecutora.sigla,
            ContratoMenor.categoria,
            func.extract("month", ContratoMenor.created_at),
        )
        .having(func.count(ContratoMenor.id) >= FRACCIONAMIENTO_MAX_CONTRATOS_MES)
        .order_by(
            UnidadEjecutora.sigla,
            func.extract("month", ContratoMenor.created_at),
        )
    )

    for row in q1.all():
        mes_num = int(row.mes_num)
        mes_label = _MES_LABELS[mes_num] if 1 <= mes_num <= 12 else str(mes_num)
        monto_acum = round(float(row.monto), 2)
        alertas.append(
            FraccionamientoAlerta(
                ue_sigla=row.ue_sigla,
                categoria=row.categoria or "SIN_CATEGORIA",
                mes=mes_label,
                cantidad_contratos=row.cantidad,
                monto_acumulado=monto_acum,
                tipo_alerta="CANTIDAD",
                detalle=(
                    f"{row.ue_sigla} registró {row.cantidad} contratos de "
                    f"'{row.categoria}' en {mes_label} {anio}, superando el "
                    f"umbral de {FRACCIONAMIENTO_MAX_CONTRATOS_MES} contratos/mes "
                    f"(posible fraccionamiento)."
                ),
            )
        )

    logger.debug("detect_fraccionamiento Rule1: %d alertas CANTIDAD", len(alertas))

    # -----------------------------------------------------------------------
    # Rule 2 — MONTO: accumulated > 8 UIT same UE + categoria per quarter
    # -----------------------------------------------------------------------
    q2 = (
        db.query(
            UnidadEjecutora.sigla.label("ue_sigla"),
            ContratoMenor.categoria.label("categoria"),
            func.extract("quarter", ContratoMenor.created_at).label("trimestre"),
            func.count(ContratoMenor.id).label("cantidad"),
            func.coalesce(func.sum(ContratoMenor.monto_estimado), 0).label("monto"),
        )
        .join(UnidadEjecutora, ContratoMenor.ue_id == UnidadEjecutora.id)
        .filter(ContratoMenor.anio == anio)
    )
    if ue_id is not None:
        q2 = q2.filter(ContratoMenor.ue_id == ue_id)

    q2 = (
        q2.group_by(
            UnidadEjecutora.id,
            UnidadEjecutora.sigla,
            ContratoMenor.categoria,
            func.extract("quarter", ContratoMenor.created_at),
        )
        .having(func.coalesce(func.sum(ContratoMenor.monto_estimado), 0) > FRACCIONAMIENTO_ACUMULADO_TRIMESTRE)
        .order_by(
            UnidadEjecutora.sigla,
            func.extract("quarter", ContratoMenor.created_at),
        )
    )

    rule2_count = 0
    for row in q2.all():
        trimestre = int(row.trimestre)
        trimestre_label = f"T{trimestre}"
        monto_acum = round(float(row.monto), 2)
        alertas.append(
            FraccionamientoAlerta(
                ue_sigla=row.ue_sigla,
                categoria=row.categoria or "SIN_CATEGORIA",
                mes=trimestre_label,
                cantidad_contratos=row.cantidad,
                monto_acumulado=monto_acum,
                tipo_alerta="MONTO",
                detalle=(
                    f"{row.ue_sigla} acumuló S/{monto_acum:,.2f} en contratos de "
                    f"'{row.categoria}' durante {trimestre_label} {anio}, superando "
                    f"el umbral de S/{UMBRAL_8_UIT:,} (8 UIT) por trimestre "
                    f"(posible fraccionamiento)."
                ),
            )
        )
        rule2_count += 1

    logger.debug("detect_fraccionamiento Rule2: %d alertas MONTO", rule2_count)
    logger.info(
        "detect_fraccionamiento: anio=%d ue_id=%s total=%d alertas",
        anio, ue_id, len(alertas),
    )
    return alertas


# ---------------------------------------------------------------------------
# Public service functions — write operations
# ---------------------------------------------------------------------------


def create_contrato(db: Session, data: ContratoMenorCreate) -> ContratoMenor:
    """Create a new minor contracting process with an auto-generated code.

    The code follows the pattern ``CM-{anio}-{seq:03d}`` where ``anio`` is
    derived from the current calendar year and ``seq`` is the next available
    sequence number for that year.

    Args:
        db: Active SQLAlchemy session.
        data: Validated creation payload from the HTTP request body.

    Returns:
        The newly persisted ``ContratoMenor`` ORM instance (with ``id`` set).

    Raises:
        HTTPException 422: If the referenced ``ue_id`` or ``meta_id`` does not
                           exist in the database.
    """
    # Validate foreign keys exist
    if not db.query(UnidadEjecutora).filter(UnidadEjecutora.id == data.ue_id).first():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"UnidadEjecutora con ID {data.ue_id} no existe.",
        )
    if not db.query(MetaPresupuestal).filter(MetaPresupuestal.id == data.meta_id).first():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"MetaPresupuestal con ID {data.meta_id} no existe.",
        )

    anio = datetime.date.today().year
    seq = _next_sequence(db, anio)
    codigo = f"CM-{anio}-{seq:03d}"

    contrato = ContratoMenor(
        codigo=codigo,
        anio=anio,
        ue_id=data.ue_id,
        meta_id=data.meta_id,
        descripcion=data.descripcion,
        tipo_objeto=data.tipo_objeto,
        categoria=data.categoria,
        estado="PENDIENTE",
        monto_estimado=data.monto_estimado,
        monto_ejecutado=None,
        proveedor_id=None,
        n_orden=None,
        n_cotizaciones=0,
    )

    db.add(contrato)
    db.commit()
    db.refresh(contrato)

    logger.info("create_contrato: created %s (id=%d)", codigo, contrato.id)
    return contrato


def update_contrato(
    db: Session,
    contrato_id: int,
    data: ContratoMenorUpdate,
) -> ContratoMenor:
    """Apply a partial update to an existing minor contracting process.

    Only fields explicitly included in the payload (non-None) are written.

    Args:
        db: Active SQLAlchemy session.
        contrato_id: Primary key of the contract to modify.
        data: Validated partial update payload.

    Returns:
        The updated and refreshed ``ContratoMenor`` ORM instance.

    Raises:
        HTTPException 404: If no contract with the given ID exists.
        HTTPException 422: If ``proveedor_id`` is supplied but does not exist.
    """
    contrato: ContratoMenor | None = (
        db.query(ContratoMenor).filter(ContratoMenor.id == contrato_id).first()
    )
    if contrato is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contrato menor con ID {contrato_id} no encontrado.",
        )

    if data.proveedor_id is not None:
        if not db.query(Proveedor).filter(Proveedor.id == data.proveedor_id).first():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Proveedor con ID {data.proveedor_id} no existe.",
            )

    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(contrato, field, value)

    db.commit()
    db.refresh(contrato)

    logger.info(
        "update_contrato: id=%d fields=%s", contrato_id, list(update_data.keys())
    )
    return contrato


def create_proceso(
    db: Session,
    contrato_id: int,
    data: ContratoMenorProcesoCreate,
) -> ContratoMenorProceso:
    """Add a new milestone step to an existing minor contracting process.

    The ``orden`` field is auto-calculated as the next sequential number
    after the highest existing step for the given contract.  A contract may
    have at most 9 milestones (the standard stepper has 9 steps).

    Args:
        db: Active SQLAlchemy session.
        contrato_id: Primary key of the parent ``ContratoMenor``.
        data: Validated milestone creation payload.

    Returns:
        The newly persisted ``ContratoMenorProceso`` ORM instance.

    Raises:
        HTTPException 404: If the parent contract does not exist.
        HTTPException 422: If the contract already has 9 milestones.
    """
    contrato: ContratoMenor | None = (
        db.query(ContratoMenor).filter(ContratoMenor.id == contrato_id).first()
    )
    if contrato is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contrato menor con ID {contrato_id} no encontrado.",
        )

    existing_count: int = (
        db.query(func.count(ContratoMenorProceso.id))
        .filter(ContratoMenorProceso.contrato_menor_id == contrato_id)
        .scalar()
        or 0
    )
    if existing_count >= 9:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"El contrato {contrato.codigo} ya tiene 9 pasos registrados "
                f"(máximo del proceso estándar ≤8 UIT)."
            ),
        )

    next_orden = existing_count + 1
    proceso = ContratoMenorProceso(
        contrato_menor_id=contrato_id,
        orden=next_orden,
        hito=data.hito,
        area_responsable=data.area_responsable,
        dias_planificados=data.dias_planificados,
        fecha_inicio=data.fecha_inicio,
        fecha_fin=None,
        estado="PENDIENTE",
    )

    db.add(proceso)
    db.commit()
    db.refresh(proceso)

    logger.info(
        "create_proceso: contrato_id=%d orden=%d hito='%s'",
        contrato_id, next_orden, data.hito,
    )
    return proceso


def update_proceso(
    db: Session,
    contrato_id: int,
    proceso_id: int,
    data: ContratoMenorProcesoUpdate,
) -> ContratoMenorProceso:
    """Apply a partial update to a single milestone step.

    Used to record the actual completion date and/or advance the step state.

    Args:
        db: Active SQLAlchemy session.
        contrato_id: Primary key of the parent ``ContratoMenor`` (used to
                     verify ownership; prevents cross-contract updates).
        proceso_id: Primary key of the ``ContratoMenorProceso`` to modify.
        data: Validated partial update payload.

    Returns:
        The updated and refreshed ``ContratoMenorProceso`` ORM instance.

    Raises:
        HTTPException 404: If the step does not exist or belongs to a
                           different contract.
    """
    proceso: ContratoMenorProceso | None = (
        db.query(ContratoMenorProceso)
        .filter(
            ContratoMenorProceso.id == proceso_id,
            ContratoMenorProceso.contrato_menor_id == contrato_id,
        )
        .first()
    )
    if proceso is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Paso {proceso_id} no encontrado en el contrato {contrato_id}."
            ),
        )

    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(proceso, field, value)

    db.commit()
    db.refresh(proceso)

    logger.info(
        "update_proceso: proceso_id=%d contrato_id=%d fields=%s",
        proceso_id, contrato_id, list(update_data.keys()),
    )
    return proceso
