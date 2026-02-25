"""
Budget Dashboard service layer.

All database access for the ``/api/presupuesto`` endpoints lives here.
Functions receive a SQLAlchemy ``Session`` and a ``FilterParams`` instance,
execute queries with proper joins and aggregations, and return schema
instances ready for serialisation by FastAPI.

Design notes
------------
- ``func.coalesce(..., 0)`` guards against NULL sums on empty result sets.
- Execution percentage is calculated in Python after aggregation to avoid
  division-by-zero inside the SQL engine.
- Every public function is synchronous (uses ``Session``, not ``AsyncSession``)
  to match the existing ``get_db`` dependency pattern in the project.
- Month labels are derived from the integer month number (1–12) using a
  static lookup list so that the API always returns Spanish abbreviations
  regardless of the database locale.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.clasificador_gasto import ClasificadorGasto
from app.models.meta_presupuestal import MetaPresupuestal
from app.models.programacion_mensual import ProgramacionMensual
from app.models.programacion_presupuestal import ProgramacionPresupuestal
from app.models.unidad_ejecutora import UnidadEjecutora
from app.schemas.common import FilterParams, PaginationParams
from app.schemas.presupuesto import (
    GraficoBarItem,
    GraficoEvolucionItem,
    KpiPresupuestoResponse,
    TablaPresupuestoResponse,
    TablaPresupuestoRow,
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


def _apply_pp_filters(query: Any, filters: FilterParams) -> Any:
    """Apply standard FilterParams constraints to a ProgramacionPresupuestal query.

    Args:
        query: An active SQLAlchemy query object targeting
               ``ProgramacionPresupuestal`` (possibly already joined).
        filters: Domain filter parameters from the HTTP request.

    Returns:
        The query with WHERE clauses appended for each non-None filter field.
    """
    if filters.anio is not None:
        query = query.filter(ProgramacionPresupuestal.anio == filters.anio)
    if filters.ue_id is not None:
        query = query.filter(ProgramacionPresupuestal.ue_id == filters.ue_id)
    if filters.meta_id is not None:
        query = query.filter(ProgramacionPresupuestal.meta_id == filters.meta_id)
    if filters.fuente_financiamiento is not None:
        query = query.filter(
            ProgramacionPresupuestal.fuente_financiamiento
            == filters.fuente_financiamiento
        )
    return query


def _safe_pct(numerator: float, denominator: float) -> float:
    """Return numerator / denominator × 100, capped at 100.0, or 0.0 if denominator is zero.

    Args:
        numerator: The dividend value (e.g. devengado).
        denominator: The divisor value (e.g. PIM).

    Returns:
        Execution percentage rounded to two decimal places (0.0–100.0).
    """
    if denominator == 0:
        return 0.0
    return round(min((numerator / denominator) * 100, 100.0), 2)


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def get_kpis(db: Session, filters: FilterParams) -> KpiPresupuestoResponse:
    """Aggregate top-level KPI figures for the Budget Dashboard header cards.

    Computes total distinct UEs and metas, plus aggregate PIM, certificado,
    and devengado within the given filter scope.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, meta, and funding-source constraints.

    Returns:
        A ``KpiPresupuestoResponse`` with all five aggregate values.
    """
    q = db.query(
        func.count(func.distinct(ProgramacionPresupuestal.ue_id)).label("total_ues"),
        func.count(func.distinct(ProgramacionPresupuestal.meta_id)).label("total_metas"),
        func.coalesce(func.sum(ProgramacionPresupuestal.pim), 0).label("pim_total"),
        func.coalesce(func.sum(ProgramacionPresupuestal.certificado), 0).label("certificado_total"),
        func.coalesce(func.sum(ProgramacionPresupuestal.compromiso_anual), 0).label("comprometido_total"),
        func.coalesce(func.sum(ProgramacionPresupuestal.devengado), 0).label("devengado_total"),
    )
    q = _apply_pp_filters(q, filters)
    row = q.one()

    pim_total = float(row.pim_total)
    certificado_total = float(row.certificado_total)
    comprometido_total = float(row.comprometido_total)
    devengado_total = float(row.devengado_total)

    logger.debug(
        "get_kpis: ues=%d metas=%d pim=%.2f",
        row.total_ues, row.total_metas, pim_total,
    )

    return KpiPresupuestoResponse(
        total_ues=row.total_ues,
        total_metas=row.total_metas,
        pim_total=pim_total,
        certificado_total=certificado_total,
        comprometido_total=comprometido_total,
        devengado_total=devengado_total,
        ejecucion_porcentaje=_safe_pct(devengado_total, pim_total),
    )


def get_grafico_pim_certificado(
    db: Session, filters: FilterParams
) -> list[GraficoBarItem]:
    """Return PIM vs certificado vs devengado grouped by UnidadEjecutora.

    Results are ordered by PIM descending so the largest units appear first
    in the bar chart, matching the frontend Recharts layout.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, meta, and funding-source constraints.

    Returns:
        A list of ``GraficoBarItem``, one per distinct UE.
    """
    q = (
        db.query(
            UnidadEjecutora.sigla.label("nombre"),
            func.coalesce(func.sum(ProgramacionPresupuestal.pim), 0).label("pim"),
            func.coalesce(func.sum(ProgramacionPresupuestal.certificado), 0).label("certificado"),
            func.coalesce(func.sum(ProgramacionPresupuestal.devengado), 0).label("devengado"),
        )
        .join(UnidadEjecutora, ProgramacionPresupuestal.ue_id == UnidadEjecutora.id)
    )
    q = _apply_pp_filters(q, filters)
    q = q.group_by(UnidadEjecutora.id, UnidadEjecutora.sigla)
    q = q.order_by(func.sum(ProgramacionPresupuestal.pim).desc())

    items: list[GraficoBarItem] = []
    for row in q.all():
        pim = float(row.pim)
        certificado = float(row.certificado)
        devengado = float(row.devengado)
        items.append(
            GraficoBarItem(
                nombre=row.nombre,
                pim=pim,
                certificado=certificado,
                devengado=devengado,
                ejecucion_porcentaje=_safe_pct(devengado, pim),
            )
        )

    logger.debug("get_grafico_pim_certificado: %d UEs returned", len(items))
    return items


def get_grafico_ejecucion(
    db: Session, filters: FilterParams
) -> list[GraficoBarItem]:
    """Return UEs ranked by execution percentage (devengado / PIM) descending.

    Intended for the "Top UEs por ejecución" horizontal bar chart.
    UEs with zero PIM are excluded to avoid meaningless 0 % entries.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, meta, and funding-source constraints.

    Returns:
        List of ``GraficoBarItem`` sorted by execution percentage, highest first.
    """
    q = (
        db.query(
            UnidadEjecutora.sigla.label("nombre"),
            func.coalesce(func.sum(ProgramacionPresupuestal.pim), 0).label("pim"),
            func.coalesce(func.sum(ProgramacionPresupuestal.certificado), 0).label("certificado"),
            func.coalesce(func.sum(ProgramacionPresupuestal.devengado), 0).label("devengado"),
        )
        .join(UnidadEjecutora, ProgramacionPresupuestal.ue_id == UnidadEjecutora.id)
    )
    q = _apply_pp_filters(q, filters)
    q = q.group_by(UnidadEjecutora.id, UnidadEjecutora.sigla)
    # Filter out UEs with no budget to avoid division by zero in the frontend
    q = q.having(func.sum(ProgramacionPresupuestal.pim) > 0)

    rows = q.all()

    items: list[GraficoBarItem] = []
    for row in rows:
        pim = float(row.pim)
        certificado = float(row.certificado)
        devengado = float(row.devengado)
        items.append(
            GraficoBarItem(
                nombre=row.nombre,
                pim=pim,
                certificado=certificado,
                devengado=devengado,
                ejecucion_porcentaje=_safe_pct(devengado, pim),
            )
        )

    # Sort in Python so we keep full precision for the percentage comparison
    items.sort(key=lambda x: x.ejecucion_porcentaje, reverse=True)
    logger.debug("get_grafico_ejecucion: %d UEs returned", len(items))
    return items


def get_grafico_devengado_mensual(
    db: Session, filters: FilterParams
) -> list[GraficoEvolucionItem]:
    """Aggregate monthly programado vs ejecutado across all matching records.

    Joins ``ProgramacionMensual`` → ``ProgramacionPresupuestal`` so that
    the same year/UE/meta/fuente filters apply uniformly.  Returns 12 items
    (one per month), with zero-filled values for months with no data.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, meta, and funding-source constraints.

    Returns:
        Exactly 12 ``GraficoEvolucionItem`` instances ordered January → December.
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
    )
    q = _apply_pp_filters(q, filters)
    # If a specific month is requested, restrict to that month only
    if filters.mes is not None:
        q = q.filter(ProgramacionMensual.mes == filters.mes)
    q = q.group_by(ProgramacionMensual.mes).order_by(ProgramacionMensual.mes)

    # Build a dict keyed by month number for O(1) lookup
    mes_data: dict[int, tuple[float, float]] = {
        row.mes: (float(row.programado), float(row.ejecutado))
        for row in q.all()
    }

    # Determine which month range to return
    if filters.mes is not None:
        mes_range = range(filters.mes, filters.mes + 1)
    else:
        mes_range = range(1, 13)

    items: list[GraficoEvolucionItem] = []
    for mes_num in mes_range:
        programado, ejecutado = mes_data.get(mes_num, (0.0, 0.0))
        items.append(
            GraficoEvolucionItem(
                mes=_MES_LABELS[mes_num],
                programado=programado,
                ejecutado=ejecutado,
            )
        )

    logger.debug("get_grafico_devengado_mensual: %d months aggregated", len(items))
    return items


def get_tabla(
    db: Session, filters: FilterParams, pagination: PaginationParams
) -> TablaPresupuestoResponse:
    """Return a paginated, fully-joined budget detail table.

    Joins ProgramacionPresupuestal → UnidadEjecutora, MetaPresupuestal, and
    ClasificadorGasto to resolve human-readable labels for every row.

    Args:
        db: Active SQLAlchemy session.
        filters: Year, UE, meta, and funding-source constraints.
        pagination: Page number and page size requested by the client.

    Returns:
        A ``TablaPresupuestoResponse`` with the current page of rows plus
        the total row count for the client to compute page count.
    """
    base_q = (
        db.query(
            ProgramacionPresupuestal.id,
            UnidadEjecutora.sigla.label("ue"),
            MetaPresupuestal.codigo.label("meta"),
            ClasificadorGasto.codigo.label("clasificador"),
            ClasificadorGasto.descripcion.label("descripcion"),
            ProgramacionPresupuestal.pim,
            ProgramacionPresupuestal.certificado,
            ProgramacionPresupuestal.devengado,
            ProgramacionPresupuestal.saldo,
        )
        .join(UnidadEjecutora, ProgramacionPresupuestal.ue_id == UnidadEjecutora.id)
        .join(MetaPresupuestal, ProgramacionPresupuestal.meta_id == MetaPresupuestal.id)
        .join(ClasificadorGasto, ProgramacionPresupuestal.clasificador_id == ClasificadorGasto.id)
    )
    base_q = _apply_pp_filters(base_q, filters)

    # Count total before pagination for the client's page navigator
    total: int = base_q.count()

    offset = (pagination.page - 1) * pagination.page_size
    page_rows = base_q.offset(offset).limit(pagination.page_size).all()

    rows: list[TablaPresupuestoRow] = []
    for row in page_rows:
        pim = float(row.pim)
        devengado = float(row.devengado)
        rows.append(
            TablaPresupuestoRow(
                id=row.id,
                ue=row.ue,
                meta=row.meta,
                clasificador=row.clasificador,
                descripcion=row.descripcion,
                pim=pim,
                certificado=float(row.certificado),
                devengado=devengado,
                saldo=float(row.saldo),
                ejecucion=_safe_pct(devengado, pim),
            )
        )

    logger.debug(
        "get_tabla: page=%d size=%d total=%d returned=%d",
        pagination.page, pagination.page_size, total, len(rows),
    )

    return TablaPresupuestoResponse(
        rows=rows,
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )
