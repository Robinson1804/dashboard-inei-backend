"""
Adquisiciones >8 UIT router.

Mounts under ``/api/adquisiciones`` (prefix set in ``main.py``).

All endpoints require a valid JWT token (``get_current_user`` dependency).
Write operations (POST, PUT) additionally require the caller to hold one of
the ``ADMIN`` or ``LOGISTICA`` roles, enforced via ``require_role``.

Filter parameters are passed as URL query strings so that the frontend can
construct bookmark-friendly links.

Endpoints
---------
GET  /kpis                   — Four KPI header cards (totals + percentages).
GET  /graficos               — Pie chart data: process distribution by estado.
GET  /tabla                  — Paginated acquisition table with joined labels.
GET  /{id}                   — Full detail: header + detalle + Gantt timeline.
POST /                       — Create a new acquisition (ADMIN/LOGISTICA).
PUT  /{id}                   — Partial update of an acquisition (ADMIN/LOGISTICA).
POST /{id}/procesos          — Add a Gantt milestone (ADMIN/LOGISTICA).
PUT  /{id}/procesos/{pid}    — Update a Gantt milestone (ADMIN/LOGISTICA).
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.adquisicion import (
    AdquisicionCreate,
    AdquisicionDetalleFullResponse,
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
from app.services.auth_service import get_current_user, require_role
from app.services import adquisicion_service
from app.services.adquisicion_service import (
    _build_joined_query,
    _resolve_adquisicion_response,
)
from app.models.adquisicion import Adquisicion as _Adquisicion

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Adquisiciones"])


# ---------------------------------------------------------------------------
# Shared dependencies — filter and pagination params from Query parameters
# ---------------------------------------------------------------------------


def _filter_params(
    anio: Annotated[
        int | None,
        Query(
            description="Año fiscal, ej. 2026. Omitir para todos los años.",
            ge=2000,
            le=2100,
        ),
    ] = None,
    ue_id: Annotated[
        int | None,
        Query(
            description="ID de la Unidad Ejecutora. Omitir para todas las UEs.",
            ge=1,
        ),
    ] = None,
    meta_id: Annotated[
        int | None,
        Query(
            description="ID de la Meta Presupuestal. Omitir para todas las metas.",
            ge=1,
        ),
    ] = None,
    estado: Annotated[
        str | None,
        Query(
            description=(
                "Estado del proceso: EN_ACTOS_PREPARATORIOS, EN_SELECCION, "
                "EN_EJECUCION, ADJUDICADO, CULMINADO, DESIERTO o NULO."
            ),
            max_length=50,
        ),
    ] = None,
    tipo_procedimiento: Annotated[
        str | None,
        Query(
            description=(
                "Tipo de procedimiento OSCE: LICITACION_PUBLICA, CONCURSO_PUBLICO, "
                "SUBASTA_INVERSA, COMPARACION_PRECIOS, CONTRATACION_DIRECTA, "
                "CATALOGO_ELECTRONICO o DIALOGO_COMPETITIVO."
            ),
            max_length=100,
        ),
    ] = None,
    fase: Annotated[
        str | None,
        Query(
            description=(
                "Fase actual del proceso: ACTUACIONES_PREPARATORIAS, "
                "SELECCION o EJECUCION_CONTRACTUAL."
            ),
            max_length=50,
        ),
    ] = None,
) -> AdquisicionFilterParams:
    """Assemble an ``AdquisicionFilterParams`` from URL query parameters.

    Used as a FastAPI dependency via ``Depends(_filter_params)`` in every
    adquisiciones read endpoint so that filter logic is centralised and
    independently testable.

    Args:
        anio: Optional fiscal year.
        ue_id: Optional UnidadEjecutora primary key.
        meta_id: Optional MetaPresupuestal primary key.
        estado: Optional process-state string.
        tipo_procedimiento: Optional OSCE procedure type.
        fase: Optional phase string.

    Returns:
        A validated ``AdquisicionFilterParams`` instance.
    """
    return AdquisicionFilterParams(
        anio=anio,
        ue_id=ue_id,
        meta_id=meta_id,
        estado=estado,
        tipo_procedimiento=tipo_procedimiento,
        fase=fase,
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
    response_model=KpiAdquisicionesResponse,
    summary="KPIs del módulo Adquisiciones >8 UIT",
    description=(
        "Retorna los indicadores clave del dashboard de adquisiciones: "
        "total de procesos, monto referencial (PIM), monto adjudicado, "
        "porcentaje de avance y distribución por estado. "
        "Acepta filtros opcionales de año, UE, meta, estado, tipo de "
        "procedimiento y fase."
    ),
    responses={
        200: {"description": "Agregados calculados exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_kpis(
    filters: Annotated[AdquisicionFilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> KpiAdquisicionesResponse:
    """Return aggregate KPI figures for the Adquisiciones dashboard header.

    Args:
        filters: Procurement-specific filter constraints.
        db: Database session injected by ``get_db``.
        _current_user: Authenticated user (validates JWT; not used directly).

    Returns:
        A ``KpiAdquisicionesResponse`` with all KPI aggregates.
    """
    logger.debug("GET /adquisiciones/kpis filters=%s", filters)
    return adquisicion_service.get_kpis(db, filters)


# ---------------------------------------------------------------------------
# GET /graficos
# ---------------------------------------------------------------------------


@router.get(
    "/graficos",
    response_model=list[GraficoAdquisicionItem],
    summary="Distribución de adquisiciones por estado (gráfico de torta)",
    description=(
        "Retorna la distribución de procesos de adquisición por estado, "
        "con cantidad, porcentaje y monto referencial por estado. "
        "Todos los estados conocidos se incluyen aunque tengan cantidad cero, "
        "para que la leyenda del gráfico sea siempre completa."
    ),
    responses={
        200: {"description": "Lista de items por estado."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_graficos(
    filters: Annotated[AdquisicionFilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[GraficoAdquisicionItem]:
    """Return per-estado distribution data for the pie chart.

    Args:
        filters: Procurement-specific filter constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        List of ``GraficoAdquisicionItem``, one per estado, sorted by
        quantity descending.
    """
    logger.debug("GET /adquisiciones/graficos filters=%s", filters)
    return adquisicion_service.get_graficos(db, filters)


# ---------------------------------------------------------------------------
# GET /tabla
# ---------------------------------------------------------------------------


@router.get(
    "/tabla",
    response_model=TablaAdquisicionesResponse,
    summary="Tabla paginada de adquisiciones >8 UIT",
    description=(
        "Retorna una página de registros de Adquisicion con labels resueltos "
        "para UE (sigla), Meta (código) y Proveedor (razón social). "
        "Soporta los mismos filtros que los demás endpoints."
    ),
    responses={
        200: {"description": "Página de registros con total y metadatos de paginación."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_tabla(
    filters: Annotated[AdquisicionFilterParams, Depends(_filter_params)],
    pagination: Annotated[PaginationParams, Depends(_pagination_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> TablaAdquisicionesResponse:
    """Return a paginated acquisitions table with all relationship labels resolved.

    Args:
        filters: Procurement-specific filter constraints.
        pagination: Page number and page size.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``TablaAdquisicionesResponse`` with the current page of rows, total
        row count, and pagination metadata.
    """
    logger.debug(
        "GET /adquisiciones/tabla filters=%s page=%d size=%d",
        filters, pagination.page, pagination.page_size,
    )
    return adquisicion_service.get_tabla(db, filters, pagination)


# ---------------------------------------------------------------------------
# GET /{id}
# ---------------------------------------------------------------------------


@router.get(
    "/{adquisicion_id}",
    response_model=AdquisicionDetalleFullResponse,
    summary="Detalle completo de una adquisición (header + detalle + Gantt)",
    description=(
        "Retorna el registro de adquisición identificado por ``id``, incluyendo "
        "su detalle extendido (número de expediente, referencias SEACE / PLADICOP) "
        "y la lista completa de hitos del cronograma Gantt (hasta 22 pasos), "
        "ordenados por campo ``orden`` ascendente."
    ),
    responses={
        200: {"description": "Detalle completo del proceso."},
        401: {"description": "Token JWT ausente o inválido."},
        404: {"description": "Adquisición no encontrada."},
    },
)
def get_detalle(
    adquisicion_id: Annotated[
        int,
        Path(description="ID de la adquisición a consultar.", ge=1),
    ],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> AdquisicionDetalleFullResponse:
    """Return the full procurement record: header, detalle, and timeline.

    Args:
        adquisicion_id: Primary key of the Adquisicion to retrieve.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        An ``AdquisicionDetalleFullResponse`` with all three sub-resources
        combined.

    Raises:
        HTTPException 404: If no Adquisicion with ``adquisicion_id`` exists.
    """
    logger.debug("GET /adquisiciones/%d", adquisicion_id)
    return adquisicion_service.get_detalle(db, adquisicion_id)


# ---------------------------------------------------------------------------
# POST /
# ---------------------------------------------------------------------------


@router.post(
    "/",
    response_model=AdquisicionResponse,
    status_code=201,
    summary="Crear nueva adquisición >8 UIT",
    description=(
        "Registra un nuevo proceso de adquisición. "
        "El código se genera automáticamente como ``ADQ-{anio}-{seq}`` cuando "
        "no se proporciona en el cuerpo. "
        "El estado inicial siempre es ``EN_ACTOS_PREPARATORIOS``. "
        "Requiere rol **ADMIN** o **LOGISTICA**."
    ),
    responses={
        201: {"description": "Adquisición creada exitosamente."},
        400: {"description": "Payload inválido (tipo_objeto o tipo_procedimiento desconocido)."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "El usuario no tiene el rol requerido."},
        409: {"description": "El código proporcionado ya está en uso."},
    },
)
def create_adquisicion(
    data: AdquisicionCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[
        Usuario, Depends(require_role("ADMIN", "LOGISTICA"))
    ],
) -> AdquisicionResponse:
    """Create a new Adquisicion record.

    Args:
        data: Validated creation payload from the request body.
        db: Database session.
        current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The created ``AdquisicionResponse`` with all fields populated.
    """
    logger.info(
        "POST /adquisiciones/ user=%s ue_id=%d",
        current_user.username, data.ue_id,
    )
    adquisicion = adquisicion_service.create_adquisicion(db, data)

    # Build the response using a single-row joined query so that denormalised
    # fields (ue_sigla, meta_codigo) are resolved for the caller immediately.
    row = (
        _build_joined_query(db)
        .filter(_Adquisicion.id == adquisicion.id)
        .first()
    )
    return _resolve_adquisicion_response(row)


# ---------------------------------------------------------------------------
# PUT /{id}
# ---------------------------------------------------------------------------


@router.put(
    "/{adquisicion_id}",
    response_model=AdquisicionResponse,
    summary="Actualizar adquisición >8 UIT",
    description=(
        "Aplica una actualización parcial al proceso de adquisición indicado. "
        "Sólo los campos incluidos en el cuerpo son modificados. "
        "Requiere rol **ADMIN** o **LOGISTICA**."
    ),
    responses={
        200: {"description": "Adquisición actualizada exitosamente."},
        400: {"description": "Valor inválido para estado, fase, tipo_objeto o tipo_procedimiento."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "El usuario no tiene el rol requerido."},
        404: {"description": "Adquisición no encontrada."},
    },
)
def update_adquisicion(
    adquisicion_id: Annotated[
        int,
        Path(description="ID de la adquisición a actualizar.", ge=1),
    ],
    data: AdquisicionUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[
        Usuario, Depends(require_role("ADMIN", "LOGISTICA"))
    ],
) -> AdquisicionResponse:
    """Apply a partial update to an existing Adquisicion.

    Args:
        adquisicion_id: Primary key of the Adquisicion to update.
        data: Validated partial-update payload from the request body.
        db: Database session.
        current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The updated ``AdquisicionResponse`` with all fields populated.

    Raises:
        HTTPException 404: If no Adquisicion with ``adquisicion_id`` exists.
    """
    logger.info(
        "PUT /adquisiciones/%d user=%s",
        adquisicion_id, current_user.username,
    )
    adquisicion_service.update_adquisicion(db, adquisicion_id, data)

    # Re-fetch with joins to resolve denormalised fields in the response
    row = (
        _build_joined_query(db)
        .filter(_Adquisicion.id == adquisicion_id)
        .first()
    )
    return _resolve_adquisicion_response(row)


# ---------------------------------------------------------------------------
# POST /{id}/procesos
# ---------------------------------------------------------------------------


@router.post(
    "/{adquisicion_id}/procesos",
    response_model=AdquisicionProcesoResponse,
    status_code=201,
    summary="Agregar hito al cronograma Gantt",
    description=(
        "Agrega un nuevo hito al cronograma Gantt de la adquisición indicada. "
        "El campo ``orden`` debe ser único dentro del proceso (1–22). "
        "Si se proveen ``fecha_inicio`` y ``dias_planificados``, la "
        "``fecha_fin`` planificada se calcula automáticamente. "
        "Requiere rol **ADMIN** o **LOGISTICA**."
    ),
    responses={
        201: {"description": "Hito creado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "El usuario no tiene el rol requerido."},
        404: {"description": "Adquisición no encontrada."},
        409: {"description": "Ya existe un hito con ese orden en esta adquisición."},
    },
)
def create_proceso(
    adquisicion_id: Annotated[
        int,
        Path(description="ID de la adquisición a la que pertenece el hito.", ge=1),
    ],
    data: AdquisicionProcesoCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[
        Usuario, Depends(require_role("ADMIN", "LOGISTICA"))
    ],
) -> AdquisicionProcesoResponse:
    """Add a new milestone to an acquisition's Gantt timeline.

    Args:
        adquisicion_id: Parent Adquisicion primary key.
        data: Validated milestone creation payload from the request body.
        db: Database session.
        current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The created ``AdquisicionProcesoResponse``.
    """
    logger.info(
        "POST /adquisiciones/%d/procesos user=%s orden=%d",
        adquisicion_id, current_user.username, data.orden,
    )
    proceso = adquisicion_service.create_proceso(db, adquisicion_id, data)
    return AdquisicionProcesoResponse.model_validate(proceso)


# ---------------------------------------------------------------------------
# PUT /{id}/procesos/{proceso_id}
# ---------------------------------------------------------------------------


@router.put(
    "/{adquisicion_id}/procesos/{proceso_id}",
    response_model=AdquisicionProcesoResponse,
    summary="Actualizar hito del cronograma Gantt",
    description=(
        "Aplica una actualización parcial al hito indicado. "
        "Típicamente se usa para registrar fechas reales de inicio/fin "
        "y cambiar el estado del hito (ej. PENDIENTE → EN_CURSO → COMPLETADO). "
        "Requiere rol **ADMIN** o **LOGISTICA**."
    ),
    responses={
        200: {"description": "Hito actualizado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "El usuario no tiene el rol requerido."},
        404: {"description": "Hito o adquisición no encontrado."},
    },
)
def update_proceso(
    adquisicion_id: Annotated[
        int,
        Path(description="ID de la adquisición propietaria del hito.", ge=1),
    ],
    proceso_id: Annotated[
        int,
        Path(description="ID del hito (AdquisicionProceso) a actualizar.", ge=1),
    ],
    data: AdquisicionProcesoUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[
        Usuario, Depends(require_role("ADMIN", "LOGISTICA"))
    ],
) -> AdquisicionProcesoResponse:
    """Apply a partial update to an existing Gantt milestone.

    Args:
        adquisicion_id: Parent Adquisicion primary key (ownership check).
        proceso_id: Primary key of the AdquisicionProceso to update.
        data: Validated partial-update payload from the request body.
        db: Database session.
        current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The updated ``AdquisicionProcesoResponse``.

    Raises:
        HTTPException 404: If no matching process/milestone combination exists.
    """
    logger.info(
        "PUT /adquisiciones/%d/procesos/%d user=%s",
        adquisicion_id, proceso_id, current_user.username,
    )
    proceso = adquisicion_service.update_proceso(
        db, adquisicion_id, proceso_id, data
    )
    return AdquisicionProcesoResponse.model_validate(proceso)
