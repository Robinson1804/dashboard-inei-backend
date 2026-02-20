"""
Actividades Operativas service layer.

All database access for the ``/api/actividades-operativas`` endpoints lives
here.  Functions receive a SQLAlchemy ``Session`` and a ``FilterParams``
instance, execute queries with proper joins and aggregations, and return
schema instances ready for serialisation by FastAPI.

Design notes
------------
- AO budget data lives in ``ProgramacionPresupuestal`` linked through
  ``MetaPresupuestal`` (shared with the AO via ``meta_id``), not directly
  on the ``ActividadOperativa`` table.
- Monthly totals are pulled from ``ProgramacionMensual`` joined to
  ``ProgramacionPresupuestal`` through the shared ``meta_id`` bridge.
- Traffic-light thresholds are imported from ``app.utils.constants`` so
  that threshold changes propagate consistently to both this service and the
  alert engine.
- ``func.coalesce(..., 0)`` guards against NULL sums on empty result sets.
- Division-by-zero is handled in Python via the ``_safe_pct`` helper.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.actividad_operativa import ActividadOperativa
from app.models.clasificador_gasto import ClasificadorGasto
from app.models.programacion_mensual import ProgramacionMensual
from app.models.programacion_presupuestal import ProgramacionPresupuestal
from app.models.unidad_ejecutora import UnidadEjecutora
from app.schemas.actividad_operativa import (
    AOTablaResponse,
    AOTablaRow,
    DrillDownAOResponse,
    DrillDownTareaItem,
    GraficoAOEvolucionItem,
    KpiAOResponse,
)
from app.schemas.common import FilterParams, PaginationParams
from app.utils.constants import (
    SEMAFORO_AMARILLO_MIN,
    SEMAFORO_VERDE_MIN,
)

logger = logging.getLogger(__name__)

# Spanish month abbreviations indexed 1–12 (index 0 unused)
_MES_LABELS: list[str] = [
    "",
    "Ene", "Feb", "Mar", "Abr", "May", "Jun",
    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_pct(numerator: float, denominator: float) -> float:
    """Return numerator / denominator × 100, or 0.0 if denominator is zero.

    Unlike the presupuesto service, AO execution is NOT capped at 100 so
    that over-execution is clearly visible in the semaforo rules.

    Args:
        numerator: Dividend (e.g. ejecutado).
        denominator: Divisor (e.g. programado).

    Returns:
        Execution percentage rounded to two decimal places. Returns 0.0
        when denominator is zero to avoid division by zero.
    """
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _semaforo(pct: float) -> str:
    """Convert an execution percentage to a traffic-light colour string.

    Uses the thresholds defined in ``app.utils.constants``:
    - >= 90 % → ``"VERDE"``
    - 70–89 % → ``"AMARILLO"``
    - < 70 %  → ``"ROJO"``

    Args:
        pct: Execution percentage (0–100+).

    Returns:
        One of ``"VERDE"``, ``"AMARILLO"``, or ``"ROJO"``.
    """
    verde_threshold = SEMAFORO_VERDE_MIN * 100      # 90.0
    amarillo_threshold = SEMAFORO_AMARILLO_MIN * 100  # 70.0

    if pct >= verde_threshold:
        return "VERDE"
    if pct >= amarillo_threshold:
        return "AMARILLO"
    return "ROJO"


def _apply_ao_filters(query: Any, filters: FilterParams) -> Any:
    """Apply FilterParams constraints scoped to ``ActividadOperativa``.

    Filters ``anio`` and ``ue_id`` map directly to ``ActividadOperativa``
    columns.  ``meta_id`` and ``fuente_financiamiento`` are ignored here
    because they belong to ``ProgramacionPresupuestal``.

    Args:
        query: An active SQLAlchemy query involving ``ActividadOperativa``.
        filters: Domain filter parameters from the HTTP request.

    Returns:
        Query with applicable WHERE clauses appended.
    """
    if filters.anio is not None:
        query = query.filter(ActividadOperativa.anio == filters.anio)
    if filters.ue_id is not None:
        query = query.filter(ActividadOperativa.ue_id == filters.ue_id)
    return query


def _build_ao_agg_subquery(db: Session, filters: FilterParams) -> Any:
    """Build a subquery that aggregates programado and ejecutado per AO.

    The join chain:
    ``ActividadOperativa``
      → ``ProgramacionPresupuestal`` (via shared meta_id + ue_id)
      → ``ProgramacionMensual``

    Args:
        db: Active SQLAlchemy session.
        filters: Year and UE constraints.

    Returns:
        A SQLAlchemy subquery with columns:
        ``ao_id``, ``programado_total``, ``ejecutado_total``.
    """
    q = (
        db.query(
            ActividadOperativa.id.label("ao_id"),
            func.coalesce(func.sum(ProgramacionMensual.programado), 0).label("programado_total"),
            func.coalesce(func.sum(ProgramacionMensual.ejecutado), 0).label("ejecutado_total"),
        )
        .join(
            ProgramacionPresupuestal,
            (ProgramacionPresupuestal.meta_id == ActividadOperativa.meta_id)
            & (ProgramacionPresupuestal.ue_id == ActividadOperativa.ue_id),
        )
        .join(
            ProgramacionMensual,
            ProgramacionMensual.programacion_presupuestal_id == ProgramacionPresupuestal.id,
        )
    )
    q = _apply_ao_filters(q, filters)
    q = q.group_by(ActividadOperativa.id)
    return q.subquery("ao_agg")


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def get_kpis(db: Session, filters: FilterParams) -> KpiAOResponse:
    """Compute KPI header figures for the Actividades Operativas Dashboard.

    Iterates over all AOs within the filter scope, calculates each one's
    execution percentage, and bins them into the VERDE / AMARILLO / ROJO
    traffic-light bands.

    Args:
        db: Active SQLAlchemy session.
        filters: Year and UE constraints.

    Returns:
        A ``KpiAOResponse`` with total AO count and per-colour counts and
        percentage shares.
    """
    ao_agg = _build_ao_agg_subquery(db, filters)

    rows = db.query(
        ao_agg.c.ao_id,
        ao_agg.c.programado_total,
        ao_agg.c.ejecutado_total,
    ).all()

    # Count AOs not represented in the aggregation (zero-budget AOs)
    all_ao_q = db.query(ActividadOperativa.id).filter(ActividadOperativa.activo.is_(True))
    all_ao_q = _apply_ao_filters(all_ao_q, filters)
    total_aos: int = all_ao_q.count()

    verdes = amarillos = rojos = 0

    for row in rows:
        pct = _safe_pct(float(row.ejecutado_total), float(row.programado_total))
        colour = _semaforo(pct)
        if colour == "VERDE":
            verdes += 1
        elif colour == "AMARILLO":
            amarillos += 1
        else:
            rojos += 1

    # AOs with no monthly data at all are classified as ROJO
    aos_with_data = len(rows)
    aos_without_data = total_aos - aos_with_data
    rojos += max(aos_without_data, 0)

    def _pct_share(count: int) -> float:
        return round((count / total_aos) * 100, 2) if total_aos > 0 else 0.0

    logger.debug(
        "get_kpis AO: total=%d verde=%d amarillo=%d rojo=%d",
        total_aos, verdes, amarillos, rojos,
    )

    return KpiAOResponse(
        total_aos=total_aos,
        verdes=verdes,
        amarillos=amarillos,
        rojos=rojos,
        porcentaje_verde=_pct_share(verdes),
        porcentaje_amarillo=_pct_share(amarillos),
        porcentaje_rojo=_pct_share(rojos),
    )


def get_programado_vs_ejecutado(
    db: Session, filters: FilterParams
) -> list[GraficoAOEvolucionItem]:
    """Aggregate monthly programado vs ejecutado across all active AOs.

    Joins through the AO → ProgramacionPresupuestal → ProgramacionMensual
    chain and groups by month number, returning 12 items with zero-fill.

    Args:
        db: Active SQLAlchemy session.
        filters: Year and UE constraints.

    Returns:
        Exactly 12 ``GraficoAOEvolucionItem`` instances, January through
        December, with zero values for months with no data.
    """
    q = (
        db.query(
            ProgramacionMensual.mes.label("mes"),
            func.coalesce(func.sum(ProgramacionMensual.programado), 0).label("programado"),
            func.coalesce(func.sum(ProgramacionMensual.ejecutado), 0).label("ejecutado"),
        )
        .join(
            ProgramacionPresupuestal,
            ProgramacionMensual.programacion_presupuestal_id == ProgramacionPresupuestal.id,
        )
        .join(
            ActividadOperativa,
            (ActividadOperativa.meta_id == ProgramacionPresupuestal.meta_id)
            & (ActividadOperativa.ue_id == ProgramacionPresupuestal.ue_id),
        )
        .filter(ActividadOperativa.activo.is_(True))
    )
    q = _apply_ao_filters(q, filters)
    q = q.group_by(ProgramacionMensual.mes).order_by(ProgramacionMensual.mes)

    mes_data: dict[int, tuple[float, float]] = {
        row.mes: (float(row.programado), float(row.ejecutado))
        for row in q.all()
    }

    items: list[GraficoAOEvolucionItem] = []
    for mes_num in range(1, 13):
        programado, ejecutado = mes_data.get(mes_num, (0.0, 0.0))
        items.append(
            GraficoAOEvolucionItem(
                mes=_MES_LABELS[mes_num],
                programado=programado,
                ejecutado=ejecutado,
            )
        )

    logger.debug("get_programado_vs_ejecutado AO: %d months aggregated", len(items))
    return items


def get_tabla(
    db: Session, filters: FilterParams, pagination: PaginationParams
) -> AOTablaResponse:
    """Return a paginated AO summary table with execution semaphore per row.

    Joins ``ActividadOperativa`` with aggregated budget totals from the
    monthly programming subquery and resolves the UE sigla label.

    Args:
        db: Active SQLAlchemy session.
        filters: Year and UE constraints.
        pagination: Page number and page size.

    Returns:
        An ``AOTablaResponse`` with the current page of rows, total row
        count, and pagination metadata.
    """
    ao_agg = _build_ao_agg_subquery(db, filters)

    base_q = (
        db.query(
            ActividadOperativa.id,
            ActividadOperativa.codigo_ceplan,
            ActividadOperativa.nombre,
            UnidadEjecutora.sigla.label("ue_sigla"),
            func.coalesce(ao_agg.c.programado_total, 0).label("programado_total"),
            func.coalesce(ao_agg.c.ejecutado_total, 0).label("ejecutado_total"),
        )
        .join(UnidadEjecutora, ActividadOperativa.ue_id == UnidadEjecutora.id)
        .outerjoin(ao_agg, ao_agg.c.ao_id == ActividadOperativa.id)
        .filter(ActividadOperativa.activo.is_(True))
    )
    base_q = _apply_ao_filters(base_q, filters)

    total: int = base_q.count()

    offset = (pagination.page - 1) * pagination.page_size
    page_rows = (
        base_q
        .order_by(ActividadOperativa.codigo_ceplan)
        .offset(offset)
        .limit(pagination.page_size)
        .all()
    )

    rows: list[AOTablaRow] = []
    for row in page_rows:
        programado = float(row.programado_total)
        ejecutado = float(row.ejecutado_total)
        pct = _safe_pct(ejecutado, programado)
        rows.append(
            AOTablaRow(
                id=row.id,
                codigo_ceplan=row.codigo_ceplan,
                nombre=row.nombre,
                ue_sigla=row.ue_sigla,
                programado_total=programado,
                ejecutado_total=ejecutado,
                ejecucion_porcentaje=pct,
                semaforo=_semaforo(pct),
            )
        )

    logger.debug(
        "get_tabla AO: page=%d size=%d total=%d returned=%d",
        pagination.page, pagination.page_size, total, len(rows),
    )

    return AOTablaResponse(
        rows=rows,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )


def get_drill_down(db: Session, ao_id: int) -> DrillDownAOResponse:
    """Return a classifier-level drill-down for a single ActividadOperativa.

    Resolves the budget breakdown at the ``ClasificadorGasto`` level by
    joining: AO → ProgramacionPresupuestal (via meta_id/ue_id) →
    ProgramacionMensual + ClasificadorGasto.

    Args:
        db: Active SQLAlchemy session.
        ao_id: Primary key of the target ``ActividadOperativa``.

    Returns:
        A ``DrillDownAOResponse`` with the AO header and a ``tareas`` list
        containing one entry per distinct classifier.

    Raises:
        HTTPException 404: If no ``ActividadOperativa`` with ``ao_id`` exists
                           or the activity is soft-deleted.
    """
    ao: ActividadOperativa | None = (
        db.query(ActividadOperativa)
        .filter(ActividadOperativa.id == ao_id, ActividadOperativa.activo.is_(True))
        .first()
    )
    if ao is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ActividadOperativa con id={ao_id} no encontrada.",
        )

    # Aggregate programado and ejecutado per classifier for this AO
    rows = (
        db.query(
            ClasificadorGasto.codigo.label("codigo"),
            ClasificadorGasto.descripcion.label("descripcion"),
            func.coalesce(func.sum(ProgramacionMensual.programado), 0).label("programado"),
            func.coalesce(func.sum(ProgramacionMensual.ejecutado), 0).label("ejecutado"),
        )
        .join(
            ProgramacionPresupuestal,
            ProgramacionPresupuestal.clasificador_id == ClasificadorGasto.id,
        )
        .join(
            ProgramacionMensual,
            ProgramacionMensual.programacion_presupuestal_id == ProgramacionPresupuestal.id,
        )
        .filter(
            ProgramacionPresupuestal.meta_id == ao.meta_id,
            ProgramacionPresupuestal.ue_id == ao.ue_id,
        )
        .group_by(ClasificadorGasto.id, ClasificadorGasto.codigo, ClasificadorGasto.descripcion)
        .order_by(ClasificadorGasto.codigo)
        .all()
    )

    tareas: list[DrillDownTareaItem] = []
    programado_total = 0.0
    ejecutado_total = 0.0

    for row in rows:
        prog = float(row.programado)
        ejec = float(row.ejecutado)
        programado_total += prog
        ejecutado_total += ejec
        tareas.append(
            DrillDownTareaItem(
                clasificador_codigo=row.codigo,
                clasificador_descripcion=row.descripcion,
                programado=prog,
                ejecutado=ejec,
                ejecucion_porcentaje=_safe_pct(ejec, prog),
            )
        )

    pct_total = _safe_pct(ejecutado_total, programado_total)

    logger.debug(
        "get_drill_down AO id=%d: %d classifiers, pct=%.2f%%",
        ao_id, len(tareas), pct_total,
    )

    return DrillDownAOResponse(
        ao_id=ao.id,
        ao_nombre=ao.nombre,
        ao_codigo=ao.codigo_ceplan,
        semaforo=_semaforo(pct_total),
        programado_total=programado_total,
        ejecutado_total=ejecutado_total,
        tareas=tareas,
    )
