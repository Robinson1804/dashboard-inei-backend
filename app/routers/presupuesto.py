"""
Budget Dashboard router.

Mounts under ``/api/presupuesto`` (prefix set in ``main.py``).

All endpoints require a valid JWT token (``get_current_user`` dependency).
Filter parameters are passed as URL query strings so that the frontend can
construct bookmark-friendly links.

Endpoints
---------
GET /kpis                     — Four KPI header cards (totals + execution %).
GET /grafico-pim-certificado  — Bar chart: PIM vs certificado vs devengado by UE.
GET /grafico-ejecucion        — Bar chart: UEs ranked by execution % descending.
GET /grafico-devengado-mensual— Line chart: monthly programado vs ejecutado.
GET /tabla                    — Paginated detail table with full joins.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.common import FilterParams, PaginationParams
from app.schemas.presupuesto import (
    GraficoBarItem,
    GraficoEvolucionItem,
    KpiPresupuestoResponse,
    TablaPresupuestoResponse,
)
from app.services.auth_service import get_current_user
from app.services import presupuesto_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Presupuesto"])


# ---------------------------------------------------------------------------
# Shared dependency — build FilterParams from Query parameters
# ---------------------------------------------------------------------------


def _filter_params(
    anio: Annotated[
        int | None,
        Query(description="Año fiscal, ej. 2026. Omitir para todos los años.", ge=2000, le=2100),
    ] = None,
    ue_id: Annotated[
        int | None,
        Query(description="ID de la Unidad Ejecutora. Omitir para todas las UEs.", ge=1),
    ] = None,
    meta_id: Annotated[
        int | None,
        Query(description="ID de la Meta Presupuestal. Omitir para todas las metas.", ge=1),
    ] = None,
    fuente: Annotated[
        str | None,
        Query(
            alias="fuente",
            description="Fuente de financiamiento, ej. 'RO', 'RDR'. Omitir para todas.",
            max_length=100,
        ),
    ] = None,
    mes: Annotated[
        int | None,
        Query(description="Mes del año (1=Enero … 12=Diciembre). Omitir para todos.", ge=1, le=12),
    ] = None,
) -> FilterParams:
    """Assemble a ``FilterParams`` instance from URL query parameters.

    Used as a FastAPI dependency via ``Depends(_filter_params)`` in every
    budget endpoint so that filter logic is centralised and testable.

    Args:
        anio: Optional fiscal year.
        ue_id: Optional UnidadEjecutora primary key.
        meta_id: Optional MetaPresupuestal primary key.
        fuente: Optional funding-source string.
        mes: Optional month number (1–12).

    Returns:
        A validated ``FilterParams`` instance.
    """
    return FilterParams(
        anio=anio,
        ue_id=ue_id,
        meta_id=meta_id,
        fuente_financiamiento=fuente,
        mes=mes,
    )


def _pagination_params(
    page: Annotated[int, Query(description="Página (base 1).", ge=1)] = 1,
    page_size: Annotated[
        int, Query(description="Registros por página (máx. 200).", ge=1, le=200)
    ] = 20,
) -> PaginationParams:
    """Assemble a ``PaginationParams`` instance from URL query parameters.

    Args:
        page: 1-based page number.
        page_size: Number of rows per page.

    Returns:
        A validated ``PaginationParams`` instance.
    """
    return PaginationParams(page=page, page_size=page_size)


# ---------------------------------------------------------------------------
# GET /kpis
# ---------------------------------------------------------------------------


@router.get(
    "/kpis",
    response_model=KpiPresupuestoResponse,
    summary="KPIs del módulo presupuestal",
    description=(
        "Retorna los cuatro indicadores clave del dashboard de presupuesto: "
        "cantidad de UEs y metas, PIM total, monto certificado y porcentaje de ejecución. "
        "Acepta filtros opcionales de año, UE, meta y fuente de financiamiento."
    ),
    responses={
        200: {"description": "Agregados calculados exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_kpis(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> KpiPresupuestoResponse:
    """Return aggregate KPI figures for the Budget Dashboard header cards.

    Args:
        filters: Year, UE, meta, and funding-source constraints.
        db: Database session injected by ``get_db``.
        _current_user: Authenticated user (validates JWT; not used directly).

    Returns:
        A ``KpiPresupuestoResponse`` with total UEs, metas, PIM, certificado,
        and execution percentage.
    """
    logger.debug("GET /presupuesto/kpis filters=%s", filters)
    return presupuesto_service.get_kpis(db, filters)


# ---------------------------------------------------------------------------
# GET /grafico-pim-certificado
# ---------------------------------------------------------------------------


@router.get(
    "/grafico-pim-certificado",
    response_model=list[GraficoBarItem],
    summary="Gráfico PIM vs Certificado por UE",
    description=(
        "Retorna los montos de PIM, certificado y devengado agrupados por Unidad Ejecutora, "
        "ordenados por PIM descendente. Usado en el gráfico de barras del dashboard."
    ),
    responses={
        200: {"description": "Lista de items por UE."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_grafico_pim_certificado(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[GraficoBarItem]:
    """Return PIM vs certificado vs devengado grouped by executing unit.

    Args:
        filters: Year, UE, meta, and funding-source constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        List of bar-chart items, one per UE, ordered by PIM descending.
    """
    logger.debug("GET /presupuesto/grafico-pim-certificado filters=%s", filters)
    return presupuesto_service.get_grafico_pim_certificado(db, filters)


# ---------------------------------------------------------------------------
# GET /grafico-ejecucion
# ---------------------------------------------------------------------------


@router.get(
    "/grafico-ejecucion",
    response_model=list[GraficoBarItem],
    summary="Ranking de ejecución por UE",
    description=(
        "Retorna las Unidades Ejecutoras ordenadas por porcentaje de ejecución "
        "(devengado / PIM) de mayor a menor. Excluye UEs sin presupuesto asignado."
    ),
    responses={
        200: {"description": "Lista de UEs ordenada por ejecución."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_grafico_ejecucion(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[GraficoBarItem]:
    """Return executing units ranked by execution percentage.

    Args:
        filters: Year, UE, meta, and funding-source constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        List of bar-chart items sorted by execution percentage (highest first).
    """
    logger.debug("GET /presupuesto/grafico-ejecucion filters=%s", filters)
    return presupuesto_service.get_grafico_ejecucion(db, filters)


# ---------------------------------------------------------------------------
# GET /grafico-devengado-mensual
# ---------------------------------------------------------------------------


@router.get(
    "/grafico-devengado-mensual",
    response_model=list[GraficoEvolucionItem],
    summary="Evolución mensual: programado vs ejecutado",
    description=(
        "Retorna los 12 meses del año con los totales de programado y ejecutado "
        "agregados de la tabla programacion_mensual. Meses sin datos devuelven cero."
    ),
    responses={
        200: {"description": "Serie mensual de 12 elementos (Ene–Dic)."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_grafico_devengado_mensual(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[GraficoEvolucionItem]:
    """Return 12 monthly data points for the programado vs ejecutado line chart.

    Args:
        filters: Year, UE, meta, and funding-source constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        Exactly 12 ``GraficoEvolucionItem`` instances, January through December.
    """
    logger.debug("GET /presupuesto/grafico-devengado-mensual filters=%s", filters)
    return presupuesto_service.get_grafico_devengado_mensual(db, filters)


# ---------------------------------------------------------------------------
# GET /tabla
# ---------------------------------------------------------------------------


@router.get(
    "/tabla",
    response_model=TablaPresupuestoResponse,
    summary="Tabla paginada de programación presupuestal",
    description=(
        "Retorna una página de registros de ProgramacionPresupuestal con joins a "
        "UnidadEjecutora, MetaPresupuestal y ClasificadorGasto. "
        "Soporta los mismos filtros que los demás endpoints de presupuesto."
    ),
    responses={
        200: {"description": "Página de registros con total y metadatos de paginación."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_tabla(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    pagination: Annotated[PaginationParams, Depends(_pagination_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> TablaPresupuestoResponse:
    """Return a paginated budget detail table with all human-readable labels resolved.

    Args:
        filters: Year, UE, meta, and funding-source constraints.
        pagination: Page number and page size.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``TablaPresupuestoResponse`` with the current page of rows, total
        row count, and pagination metadata.
    """
    logger.debug(
        "GET /presupuesto/tabla filters=%s page=%d size=%d",
        filters, pagination.page, pagination.page_size,
    )
    return presupuesto_service.get_tabla(db, filters, pagination)
