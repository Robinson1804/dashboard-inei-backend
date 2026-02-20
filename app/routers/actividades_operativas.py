"""
Actividades Operativas router.

Mounts under ``/api/actividades-operativas`` (prefix set in ``main.py``).

All endpoints require a valid JWT token (``get_current_user`` dependency).
Filter parameters are passed as URL query strings so that the frontend can
construct bookmark-friendly links.

Endpoints
---------
GET /kpis                      — KPI header cards (total AOs + semaphore counts).
GET /programado-vs-ejecutado   — Monthly line chart data (12 months).
GET /tabla                     — Paginated AO summary table with semaphore per row.
GET /{id}/drill-down           — Classifier-level drill-down for a single AO.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.actividad_operativa import (
    AOTablaResponse,
    DrillDownAOResponse,
    GraficoAOEvolucionItem,
    KpiAOResponse,
)
from app.schemas.common import FilterParams, PaginationParams
from app.services.auth_service import get_current_user
from app.services import ao_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Actividades Operativas"])


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
) -> FilterParams:
    """Assemble ``FilterParams`` from URL query parameters for AO endpoints.

    AO-scoped filters only use ``anio`` and ``ue_id``; ``meta_id`` and
    ``fuente_financiamiento`` are available on ``FilterParams`` but are not
    exposed here because AOs do not filter by those dimensions directly.

    Args:
        anio: Optional fiscal year.
        ue_id: Optional UnidadEjecutora primary key.

    Returns:
        A validated ``FilterParams`` instance with only AO-relevant fields set.
    """
    return FilterParams(anio=anio, ue_id=ue_id)


def _pagination_params(
    page: Annotated[int, Query(description="Página (base 1).", ge=1)] = 1,
    page_size: Annotated[
        int, Query(description="Registros por página (máx. 200).", ge=1, le=200)
    ] = 20,
) -> PaginationParams:
    """Assemble ``PaginationParams`` from URL query parameters.

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
    response_model=KpiAOResponse,
    summary="KPIs del módulo de Actividades Operativas",
    description=(
        "Retorna el total de AOs activas y su distribución por semáforo de ejecución "
        "(VERDE >= 90%, AMARILLO 70-89%, ROJO < 70%). "
        "Acepta filtros opcionales de año y UE."
    ),
    responses={
        200: {"description": "KPIs calculados exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_kpis(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> KpiAOResponse:
    """Return aggregate KPI figures for the Actividades Operativas header cards.

    Args:
        filters: Year and UE constraints.
        db: Database session injected by ``get_db``.
        _current_user: Authenticated user (validates JWT; not used directly).

    Returns:
        A ``KpiAOResponse`` with total AO count and per-colour semaphore
        breakdown plus percentage shares.
    """
    logger.debug("GET /actividades-operativas/kpis filters=%s", filters)
    return ao_service.get_kpis(db, filters)


# ---------------------------------------------------------------------------
# GET /programado-vs-ejecutado
# ---------------------------------------------------------------------------


@router.get(
    "/programado-vs-ejecutado",
    response_model=list[GraficoAOEvolucionItem],
    summary="Evolución mensual: programado vs ejecutado (AOs)",
    description=(
        "Retorna los 12 meses del año con los totales de programado y ejecutado "
        "agregados de todas las Actividades Operativas dentro del filtro. "
        "Meses sin datos devuelven cero."
    ),
    responses={
        200: {"description": "Serie mensual de 12 elementos (Ene–Dic)."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_programado_vs_ejecutado(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[GraficoAOEvolucionItem]:
    """Return monthly programado vs ejecutado evolution for the AO line chart.

    Args:
        filters: Year and UE constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        Exactly 12 ``GraficoAOEvolucionItem`` instances, January through December.
    """
    logger.debug("GET /actividades-operativas/programado-vs-ejecutado filters=%s", filters)
    return ao_service.get_programado_vs_ejecutado(db, filters)


# ---------------------------------------------------------------------------
# GET /tabla
# ---------------------------------------------------------------------------


@router.get(
    "/tabla",
    response_model=AOTablaResponse,
    summary="Tabla paginada de Actividades Operativas",
    description=(
        "Retorna una página de AOs con sus totales de ejecución presupuestal "
        "y el semáforo calculado (VERDE/AMARILLO/ROJO). "
        "Soporta los mismos filtros de año y UE."
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
) -> AOTablaResponse:
    """Return a paginated AO summary table with execution semaphore per row.

    Args:
        filters: Year and UE constraints.
        pagination: Page number and page size.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        An ``AOTablaResponse`` with the current page of rows, total row count,
        and pagination metadata.
    """
    logger.debug(
        "GET /actividades-operativas/tabla filters=%s page=%d size=%d",
        filters, pagination.page, pagination.page_size,
    )
    return ao_service.get_tabla(db, filters, pagination)


# ---------------------------------------------------------------------------
# GET /{id}/drill-down
# ---------------------------------------------------------------------------


@router.get(
    "/{ao_id}/drill-down",
    response_model=DrillDownAOResponse,
    summary="Drill-down de una Actividad Operativa al nivel de clasificador",
    description=(
        "Retorna el detalle de ejecución presupuestal de una AO específica "
        "desglosado por clasificador de gasto. Incluye el semáforo general "
        "y el semáforo individual por clasificador."
    ),
    responses={
        200: {"description": "Drill-down calculado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        404: {"description": "ActividadOperativa no encontrada."},
    },
)
def get_drill_down(
    ao_id: Annotated[
        int,
        Path(description="ID de la ActividadOperativa.", ge=1),
    ],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> DrillDownAOResponse:
    """Return classifier-level drill-down data for a single ActividadOperativa.

    Args:
        ao_id: Primary key of the target ActividadOperativa.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``DrillDownAOResponse`` with the AO header summary and a ``tareas``
        list containing one entry per distinct classifier.

    Raises:
        HTTPException 404: If the requested AO does not exist or is inactive.
    """
    logger.debug("GET /actividades-operativas/%d/drill-down", ao_id)
    return ao_service.get_drill_down(db, ao_id)
