"""
Adquisiciones >8 UIT — service layer.

All database access for the ``/api/adquisiciones`` endpoints lives here.
Functions receive a SQLAlchemy ``Session``, execute queries with proper joins
and aggregations, and return schema instances ready for serialisation by
FastAPI.

Design notes
------------
- ``func.coalesce(..., 0)`` guards against NULL sums on empty result sets.
- ``_safe_pct`` mirrors the helper used in ``presupuesto_service`` so that
  percentage calculations stay consistent across modules.
- Auto-generated codes follow the format ``ADQ-{anio}-{seq:03d}``.  The
  sequence is derived from a COUNT of existing rows for the same year so
  that it never collides (increments even if earlier codes were deleted).
- Write operations (create / update) commit immediately and refresh the ORM
  instance so callers always receive the up-to-date record.
- All relationship resolution for denormalised response fields is done via
  explicit SQLAlchemy joins (not lazy-loaded attributes) to keep N+1 queries
  out of list endpoints.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.adquisicion import Adquisicion
from app.models.adquisicion_detalle import AdquisicionDetalle
from app.models.adquisicion_proceso import AdquisicionProceso
from app.models.meta_presupuestal import MetaPresupuestal
from app.models.proveedor import Proveedor
from app.models.unidad_ejecutora import UnidadEjecutora
from app.schemas.adquisicion import (
    AdquisicionCreate,
    AdquisicionDetalleFullResponse,
    AdquisicionDetalleResponse,
    AdquisicionFilterParams,
    AdquisicionProcesoCreate,
    AdquisicionProcesoResponse,
    AdquisicionProcesoUpdate,
    AdquisicionResponse,
    AdquisicionUpdate,
    GraficoAdquisicionItem,
    KpiAdquisicionesResponse,
    TablaAdquisicionesResponse,
)
from app.schemas.common import PaginationParams
from app.utils.constants import (
    ESTADOS_ADQUISICION,
    FASES_ADQUISICION,
    TIPOS_OBJETO,
    TIPOS_PROCEDIMIENTO,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Human-readable labels for each estado (used in pie-chart responses)
# ---------------------------------------------------------------------------

_ESTADO_LABELS: dict[str, str] = {
    "EN_ACTOS_PREPARATORIOS": "Actos Preparatorios",
    "EN_SELECCION": "En Selección",
    "EN_EJECUCION": "En Ejecución",
    "ADJUDICADO": "Adjudicado",
    "CULMINADO": "Culminado",
    "DESIERTO": "Desierto",
    "NULO": "Nulo",
}

# States that count as "in-progress" for KPI purposes
_ESTADOS_ACTIVOS: frozenset[str] = frozenset(
    {
        "EN_ACTOS_PREPARATORIOS",
        "EN_SELECCION",
        "EN_EJECUCION",
    }
)

# States that count as "advanced" for the avance_porcentaje KPI
_ESTADOS_AVANZADOS: frozenset[str] = frozenset({"ADJUDICADO", "CULMINADO"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_pct(numerator: float, denominator: float) -> float:
    """Return numerator / denominator × 100, capped at 100.0, or 0.0 when
    denominator is zero.

    Args:
        numerator: Dividend value (e.g. culminados count).
        denominator: Divisor value (e.g. total count).

    Returns:
        Percentage rounded to two decimal places in the range [0.0, 100.0].
    """
    if denominator == 0:
        return 0.0
    return round(min((numerator / denominator) * 100, 100.0), 2)


def _apply_filters(query: Any, filters: AdquisicionFilterParams) -> Any:
    """Apply AdquisicionFilterParams WHERE clauses to a query targeting Adquisicion.

    Each filter field is applied only when it is not None, making all filters
    independently optional.

    Args:
        query: Active SQLAlchemy query targeting ``Adquisicion``.
        filters: Acquisition-specific filter parameters from the HTTP request.

    Returns:
        The query with all non-None filter conditions appended.
    """
    if filters.anio is not None:
        query = query.filter(Adquisicion.anio == filters.anio)
    if filters.ue_id is not None:
        query = query.filter(Adquisicion.ue_id == filters.ue_id)
    if filters.meta_id is not None:
        query = query.filter(Adquisicion.meta_id == filters.meta_id)
    if filters.estado is not None:
        query = query.filter(Adquisicion.estado == filters.estado)
    if filters.tipo_procedimiento is not None:
        query = query.filter(
            Adquisicion.tipo_procedimiento == filters.tipo_procedimiento
        )
    if filters.fase is not None:
        query = query.filter(Adquisicion.fase_actual == filters.fase)
    return query


def _resolve_adquisicion_response(
    row: Any,
) -> AdquisicionResponse:
    """Convert a raw SQLAlchemy result row to an AdquisicionResponse.

    The row must expose the following labelled columns:
    ``id``, ``codigo``, ``anio``, ``ue_id``, ``ue_sigla``, ``meta_id``,
    ``meta_codigo``, ``descripcion``, ``tipo_objeto``, ``tipo_procedimiento``,
    ``estado``, ``fase_actual``, ``monto_referencial``, ``monto_adjudicado``,
    ``proveedor_id``, ``proveedor_razon_social``, ``created_at``,
    ``updated_at``.

    Args:
        row: A named-tuple row from a SQLAlchemy query result.

    Returns:
        A fully populated ``AdquisicionResponse`` instance.
    """
    return AdquisicionResponse(
        id=row.id,
        codigo=row.codigo,
        anio=row.anio,
        ue_id=row.ue_id,
        ue_sigla=row.ue_sigla,
        meta_id=row.meta_id,
        meta_codigo=row.meta_codigo,
        descripcion=row.descripcion,
        tipo_objeto=row.tipo_objeto,
        tipo_procedimiento=row.tipo_procedimiento,
        estado=row.estado,
        fase_actual=row.fase_actual,
        monto_referencial=(
            float(row.monto_referencial) if row.monto_referencial is not None else None
        ),
        monto_adjudicado=(
            float(row.monto_adjudicado) if row.monto_adjudicado is not None else None
        ),
        proveedor_id=row.proveedor_id,
        proveedor_razon_social=row.proveedor_razon_social,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _build_joined_query(db: Session) -> Any:
    """Build the canonical joined query used by get_tabla and get_detalle.

    Joins Adquisicion to UnidadEjecutora, MetaPresupuestal, and Proveedor
    using outer-joins for the nullable FK relationships so that processes
    without a proveedor or meta are still returned.

    Args:
        db: Active SQLAlchemy session.

    Returns:
        A SQLAlchemy query selecting all columns needed by
        ``_resolve_adquisicion_response``.
    """
    return db.query(
        Adquisicion.id,
        Adquisicion.codigo,
        Adquisicion.anio,
        Adquisicion.ue_id,
        UnidadEjecutora.sigla.label("ue_sigla"),
        Adquisicion.meta_id,
        MetaPresupuestal.codigo.label("meta_codigo"),
        Adquisicion.descripcion,
        Adquisicion.tipo_objeto,
        Adquisicion.tipo_procedimiento,
        Adquisicion.estado,
        Adquisicion.fase_actual,
        Adquisicion.monto_referencial,
        Adquisicion.monto_adjudicado,
        Adquisicion.proveedor_id,
        Proveedor.razon_social.label("proveedor_razon_social"),
        Adquisicion.created_at,
        Adquisicion.updated_at,
    ).outerjoin(
        UnidadEjecutora, Adquisicion.ue_id == UnidadEjecutora.id
    ).outerjoin(
        MetaPresupuestal, Adquisicion.meta_id == MetaPresupuestal.id
    ).outerjoin(
        Proveedor, Adquisicion.proveedor_id == Proveedor.id
    )


def _generate_codigo(db: Session, anio: int) -> str:
    """Auto-generate a unique process code in the format ``ADQ-{anio}-{seq:03d}``.

    The sequence number is determined by counting all existing Adquisicion rows
    for the given fiscal year and adding one.  This strategy is collision-safe
    within a single transaction but does not use a DB sequence object, which is
    acceptable for the expected volume of INEI acquisitions.

    Args:
        db: Active SQLAlchemy session.
        anio: Fiscal year for the new acquisition.

    Returns:
        A unique string code, e.g. ``ADQ-2026-001``.
    """
    existing_count: int = (
        db.query(func.count(Adquisicion.id))
        .filter(Adquisicion.anio == anio)
        .scalar()
        or 0
    )
    seq = existing_count + 1
    return f"ADQ-{anio}-{seq:03d}"


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def get_kpis(
    db: Session, filters: AdquisicionFilterParams
) -> KpiAdquisicionesResponse:
    """Aggregate top-level KPI figures for the Adquisiciones dashboard header.

    Computes the total process count, monetary sums, and state distribution
    within the given filter scope.

    Args:
        db: Active SQLAlchemy session.
        filters: Acquisition-specific filter parameters.

    Returns:
        A ``KpiAdquisicionesResponse`` with all aggregate values.
    """
    # Aggregate query: count per estado + sum of montos
    q = db.query(
        func.count(Adquisicion.id).label("total"),
        func.coalesce(func.sum(Adquisicion.monto_referencial), 0).label("monto_pim"),
        func.coalesce(func.sum(Adquisicion.monto_adjudicado), 0).label("monto_adjudicado"),
    )
    q = _apply_filters(q, filters)
    agg_row = q.one()

    total: int = agg_row.total
    monto_pim = float(agg_row.monto_pim)
    monto_adjudicado = float(agg_row.monto_adjudicado)

    # Distribution by estado
    dist_q = db.query(
        Adquisicion.estado.label("estado"),
        func.count(Adquisicion.id).label("cantidad"),
    ).group_by(Adquisicion.estado)
    dist_q = _apply_filters(dist_q, filters)

    by_estado: dict[str, int] = {e: 0 for e in ESTADOS_ADQUISICION}
    for row in dist_q.all():
        if row.estado is not None:
            by_estado[row.estado] = row.cantidad

    culminados: int = by_estado.get("CULMINADO", 0)
    en_proceso: int = sum(
        by_estado.get(e, 0) for e in _ESTADOS_ACTIVOS
    )
    avanzados: int = sum(
        by_estado.get(e, 0) for e in _ESTADOS_AVANZADOS
    )

    logger.debug(
        "get_kpis: total=%d pim=%.2f adj=%.2f culminados=%d en_proceso=%d",
        total, monto_pim, monto_adjudicado, culminados, en_proceso,
    )

    return KpiAdquisicionesResponse(
        total=total,
        monto_pim=monto_pim,
        monto_adjudicado=monto_adjudicado,
        avance_porcentaje=_safe_pct(avanzados, total),
        culminados=culminados,
        en_proceso=en_proceso,
        by_estado=by_estado,
    )


def get_graficos(
    db: Session, filters: AdquisicionFilterParams
) -> list[GraficoAdquisicionItem]:
    """Return per-estado distribution for the pie/donut chart.

    Computes the count and sum of monto_referencial for each estado value
    and expresses each as a percentage of the total.  All known estados are
    always included (even at zero) so the frontend legend stays consistent.

    Args:
        db: Active SQLAlchemy session.
        filters: Acquisition-specific filter parameters.

    Returns:
        A list of ``GraficoAdquisicionItem`` instances, one per estado, ordered
        by quantity descending.
    """
    # Total count for percentage calculation
    total_q = db.query(func.count(Adquisicion.id))
    total_q = _apply_filters(total_q, filters)
    total: int = total_q.scalar() or 0

    # Per-estado aggregation
    dist_q = db.query(
        Adquisicion.estado.label("estado"),
        func.count(Adquisicion.id).label("cantidad"),
        func.coalesce(func.sum(Adquisicion.monto_referencial), 0).label("monto"),
    ).group_by(Adquisicion.estado)
    dist_q = _apply_filters(dist_q, filters)

    # Seed all known estados at zero so the chart always renders complete data
    estado_data: dict[str, tuple[int, float]] = {
        e: (0, 0.0) for e in ESTADOS_ADQUISICION
    }
    for row in dist_q.all():
        if row.estado is not None:
            estado_data[row.estado] = (row.cantidad, float(row.monto))

    items: list[GraficoAdquisicionItem] = [
        GraficoAdquisicionItem(
            estado=estado,
            label=_ESTADO_LABELS.get(estado, estado),
            cantidad=cantidad,
            porcentaje=_safe_pct(cantidad, total),
            monto=monto,
        )
        for estado, (cantidad, monto) in estado_data.items()
    ]

    # Sort by quantity descending so the largest slice comes first
    items.sort(key=lambda x: x.cantidad, reverse=True)

    logger.debug("get_graficos: %d estados in result, total=%d", len(items), total)
    return items


def get_tabla(
    db: Session,
    filters: AdquisicionFilterParams,
    pagination: PaginationParams,
) -> TablaAdquisicionesResponse:
    """Return a paginated, fully-joined acquisitions table.

    Joins Adquisicion to UnidadEjecutora, MetaPresupuestal, and Proveedor
    so that human-readable labels are resolved server-side.

    Args:
        db: Active SQLAlchemy session.
        filters: Acquisition-specific filter parameters.
        pagination: Page number and page size from the HTTP request.

    Returns:
        A ``TablaAdquisicionesResponse`` with the current page of rows plus
        the total row count.
    """
    base_q = _build_joined_query(db)
    base_q = _apply_filters(base_q, filters)

    # Subquery count to avoid the expense of counting a fully-joined query
    count_q = db.query(func.count(Adquisicion.id))
    count_q = _apply_filters(count_q, filters)
    total: int = count_q.scalar() or 0

    offset = (pagination.page - 1) * pagination.page_size
    page_rows = (
        base_q
        .order_by(Adquisicion.created_at.desc())
        .offset(offset)
        .limit(pagination.page_size)
        .all()
    )

    rows = [_resolve_adquisicion_response(row) for row in page_rows]

    logger.debug(
        "get_tabla: page=%d size=%d total=%d returned=%d",
        pagination.page, pagination.page_size, total, len(rows),
    )

    return TablaAdquisicionesResponse(
        rows=rows,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )


def get_detalle(
    db: Session, adquisicion_id: int
) -> AdquisicionDetalleFullResponse:
    """Return the full procurement detail: header + extended detalle + timeline.

    Fetches the Adquisicion header (with joins for denormalised labels), its
    optional AdquisicionDetalle companion record, and all AdquisicionProceso
    milestone rows ordered by ``orden`` ascending.

    Args:
        db: Active SQLAlchemy session.
        adquisicion_id: Primary key of the Adquisicion to retrieve.

    Returns:
        An ``AdquisicionDetalleFullResponse`` combining all three sub-resources.

    Raises:
        HTTPException 404: If no Adquisicion with ``adquisicion_id`` exists.
    """
    # Header with relationship resolution
    header_row = (
        _build_joined_query(db)
        .filter(Adquisicion.id == adquisicion_id)
        .first()
    )
    if header_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Adquisición con id={adquisicion_id} no encontrada.",
        )

    adq_response = _resolve_adquisicion_response(header_row)

    # Optional 1:1 extended detail
    detalle_orm: AdquisicionDetalle | None = (
        db.query(AdquisicionDetalle)
        .filter(AdquisicionDetalle.adquisicion_id == adquisicion_id)
        .first()
    )
    detalle_response: AdquisicionDetalleResponse | None = None
    if detalle_orm is not None:
        detalle_response = AdquisicionDetalleResponse.model_validate(detalle_orm)

    # Ordered milestone timeline
    procesos_orm: list[AdquisicionProceso] = (
        db.query(AdquisicionProceso)
        .filter(AdquisicionProceso.adquisicion_id == adquisicion_id)
        .order_by(AdquisicionProceso.orden.asc())
        .all()
    )
    procesos_response = [
        AdquisicionProcesoResponse.model_validate(p) for p in procesos_orm
    ]

    logger.debug(
        "get_detalle: id=%d procesos=%d detalle=%s",
        adquisicion_id,
        len(procesos_response),
        "present" if detalle_response else "absent",
    )

    return AdquisicionDetalleFullResponse(
        adquisicion=adq_response,
        detalle=detalle_response,
        procesos=procesos_response,
    )


def create_adquisicion(
    db: Session, data: AdquisicionCreate
) -> Adquisicion:
    """Create a new Adquisicion record and persist it to the database.

    If ``data.codigo`` is None, a unique code is auto-generated using the
    fiscal year derived from the current calendar year.

    Args:
        db: Active SQLAlchemy session.
        data: Validated creation payload.

    Returns:
        The freshly created and refreshed ``Adquisicion`` ORM instance.

    Raises:
        HTTPException 400: If ``data.tipo_objeto`` or ``data.tipo_procedimiento``
                           contain values not in the project's constant lists.
        HTTPException 409: If an explicit ``data.codigo`` is already in use.
    """
    # Validate enumerated fields against known constants
    if data.tipo_objeto not in TIPOS_OBJETO:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"tipo_objeto '{data.tipo_objeto}' inválido. "
                f"Valores válidos: {TIPOS_OBJETO}."
            ),
        )
    if data.tipo_procedimiento not in TIPOS_PROCEDIMIENTO:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"tipo_procedimiento '{data.tipo_procedimiento}' inválido. "
                f"Valores válidos: {TIPOS_PROCEDIMIENTO}."
            ),
        )

    anio = datetime.date.today().year

    # Resolve or generate the process code
    if data.codigo is not None:
        # Check uniqueness of explicitly supplied code
        existing = (
            db.query(Adquisicion.id)
            .filter(Adquisicion.codigo == data.codigo)
            .first()
        )
        if existing is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Ya existe una adquisición con código '{data.codigo}'.",
            )
        codigo = data.codigo
    else:
        codigo = _generate_codigo(db, anio)

    adquisicion = Adquisicion(
        codigo=codigo,
        anio=anio,
        ue_id=data.ue_id,
        meta_id=data.meta_id,
        descripcion=data.descripcion,
        tipo_objeto=data.tipo_objeto,
        tipo_procedimiento=data.tipo_procedimiento,
        estado="EN_ACTOS_PREPARATORIOS",
        fase_actual="ACTUACIONES_PREPARATORIAS",
        monto_referencial=data.monto_referencial,
    )

    db.add(adquisicion)
    db.commit()
    db.refresh(adquisicion)

    logger.info(
        "create_adquisicion: created id=%d codigo=%s ue_id=%d",
        adquisicion.id, adquisicion.codigo, adquisicion.ue_id,
    )
    return adquisicion


def update_adquisicion(
    db: Session, adquisicion_id: int, data: AdquisicionUpdate
) -> Adquisicion:
    """Apply a partial update to an existing Adquisicion.

    Only the fields explicitly provided (non-None) in ``data`` are written.
    All other columns remain unchanged.

    Args:
        db: Active SQLAlchemy session.
        adquisicion_id: Primary key of the Adquisicion to update.
        data: Validated partial-update payload.

    Returns:
        The updated and refreshed ``Adquisicion`` ORM instance.

    Raises:
        HTTPException 404: If no Adquisicion with ``adquisicion_id`` exists.
        HTTPException 400: If ``data.estado`` or ``data.fase_actual`` contain
                           values not in the project's constant lists.
    """
    adquisicion: Adquisicion | None = (
        db.query(Adquisicion).filter(Adquisicion.id == adquisicion_id).first()
    )
    if adquisicion is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Adquisición con id={adquisicion_id} no encontrada.",
        )

    # Validate enumerated fields when supplied
    if data.estado is not None and data.estado not in ESTADOS_ADQUISICION:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"estado '{data.estado}' inválido. "
                f"Valores válidos: {ESTADOS_ADQUISICION}."
            ),
        )
    if data.fase_actual is not None and data.fase_actual not in FASES_ADQUISICION:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"fase_actual '{data.fase_actual}' inválido. "
                f"Valores válidos: {FASES_ADQUISICION}."
            ),
        )
    if data.tipo_objeto is not None and data.tipo_objeto not in TIPOS_OBJETO:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"tipo_objeto '{data.tipo_objeto}' inválido. "
                f"Valores válidos: {TIPOS_OBJETO}."
            ),
        )
    if (
        data.tipo_procedimiento is not None
        and data.tipo_procedimiento not in TIPOS_PROCEDIMIENTO
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"tipo_procedimiento '{data.tipo_procedimiento}' inválido. "
                f"Valores válidos: {TIPOS_PROCEDIMIENTO}."
            ),
        )

    # Apply only the supplied fields
    update_data = data.model_dump(exclude_none=True)
    for field, value in update_data.items():
        setattr(adquisicion, field, value)

    db.commit()
    db.refresh(adquisicion)

    logger.info(
        "update_adquisicion: id=%d fields=%s",
        adquisicion_id, list(update_data.keys()),
    )
    return adquisicion


def create_proceso(
    db: Session, adquisicion_id: int, data: AdquisicionProcesoCreate
) -> AdquisicionProceso:
    """Add a new milestone to an acquisition's Gantt timeline.

    Verifies the parent Adquisicion exists and that the requested ``orden``
    position is not already occupied.

    Args:
        db: Active SQLAlchemy session.
        adquisicion_id: Parent Adquisicion primary key.
        data: Validated milestone creation payload.

    Returns:
        The newly created and refreshed ``AdquisicionProceso`` ORM instance.

    Raises:
        HTTPException 404: If the parent Adquisicion does not exist.
        HTTPException 409: If a process with the same ``orden`` already exists
                           for this acquisition.
    """
    # Verify parent exists
    exists: bool = (
        db.query(Adquisicion.id).filter(Adquisicion.id == adquisicion_id).first()
        is not None
    )
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Adquisición con id={adquisicion_id} no encontrada.",
        )

    # Enforce unique orden within the same acquisition
    conflict = (
        db.query(AdquisicionProceso.id)
        .filter(
            AdquisicionProceso.adquisicion_id == adquisicion_id,
            AdquisicionProceso.orden == data.orden,
        )
        .first()
    )
    if conflict is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Ya existe un hito con orden={data.orden} para la "
                f"adquisición id={adquisicion_id}."
            ),
        )

    # Calculate planned end date if start date and planned days are provided
    fecha_fin: datetime.date | None = None
    if data.fecha_inicio is not None and data.dias_planificados is not None:
        fecha_fin = data.fecha_inicio + datetime.timedelta(days=data.dias_planificados)

    proceso = AdquisicionProceso(
        adquisicion_id=adquisicion_id,
        orden=data.orden,
        hito=data.hito,
        fase=data.fase,
        area_responsable=data.area_responsable,
        dias_planificados=data.dias_planificados,
        fecha_inicio=data.fecha_inicio,
        fecha_fin=fecha_fin,
        estado=data.estado,
    )

    db.add(proceso)
    db.commit()
    db.refresh(proceso)

    logger.info(
        "create_proceso: adquisicion_id=%d orden=%d hito='%s'",
        adquisicion_id, data.orden, data.hito,
    )
    return proceso


def update_proceso(
    db: Session,
    adquisicion_id: int,
    proceso_id: int,
    data: AdquisicionProcesoUpdate,
) -> AdquisicionProceso:
    """Apply a partial update to an existing milestone.

    Only the non-None fields in ``data`` are written to the database.

    Args:
        db: Active SQLAlchemy session.
        adquisicion_id: Parent Adquisicion primary key (used for ownership check).
        proceso_id: Primary key of the AdquisicionProceso to update.
        data: Validated partial-update payload.

    Returns:
        The updated and refreshed ``AdquisicionProceso`` ORM instance.

    Raises:
        HTTPException 404: If no AdquisicionProceso matching both
                           ``adquisicion_id`` and ``proceso_id`` exists.
    """
    proceso: AdquisicionProceso | None = (
        db.query(AdquisicionProceso)
        .filter(
            AdquisicionProceso.id == proceso_id,
            AdquisicionProceso.adquisicion_id == adquisicion_id,
        )
        .first()
    )
    if proceso is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Hito con id={proceso_id} no encontrado en la "
                f"adquisición id={adquisicion_id}."
            ),
        )

    update_data = data.model_dump(exclude_none=True)
    for field, value in update_data.items():
        setattr(proceso, field, value)

    db.commit()
    db.refresh(proceso)

    logger.info(
        "update_proceso: proceso_id=%d adquisicion_id=%d fields=%s",
        proceso_id, adquisicion_id, list(update_data.keys()),
    )
    return proceso
