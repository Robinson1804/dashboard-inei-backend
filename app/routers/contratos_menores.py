"""
Contratos Menores ≤8 UIT router.

Mounts under ``/api/contratos-menores`` (prefix set in ``main.py``).

All endpoints require a valid JWT token (``get_current_user`` dependency).
Write operations (POST, PUT) additionally enforce that the caller holds one
of the ``ADMIN`` or ``LOGISTICA`` roles via ``require_role``.

Endpoints
---------
GET  /kpis                         — KPI header cards (totals + alerts).
GET  /graficos                     — Distribution charts by estado and tipo_objeto.
GET  /tabla                        — Paginated detail table.
GET  /fraccionamiento              — Fraccionamiento detection alerts.
GET  /{id}                         — Full contract detail with stepper timeline.
POST /                             — Create new minor contract (ADMIN | LOGISTICA).
PUT  /{id}                         — Partial update of a contract (ADMIN | LOGISTICA).
POST /{id}/procesos                — Add a milestone step (ADMIN | LOGISTICA).
PUT  /{id}/procesos/{proceso_id}   — Update a milestone step (ADMIN | LOGISTICA).
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.contrato_menor import ContratoMenor
from app.models.contrato_menor_proceso import ContratoMenorProceso
from app.models.usuario import Usuario
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
from app.services.auth_service import get_current_user, require_role
from app.services import contrato_menor_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Contratos Menores"])


# ---------------------------------------------------------------------------
# Shared dependencies — filter and pagination params
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
) -> FilterParams:
    """Assemble ``FilterParams`` from URL query strings.

    The ``fuente_financiamiento`` axis is not applicable to minor contracts and
    is intentionally excluded from this dependency.

    Args:
        anio: Optional fiscal year constraint.
        ue_id: Optional UnidadEjecutora primary key.
        meta_id: Optional MetaPresupuestal primary key.

    Returns:
        A validated ``FilterParams`` instance.
    """
    return FilterParams(anio=anio, ue_id=ue_id, meta_id=meta_id)


def _pagination_params(
    page: Annotated[int, Query(description="Página (base 1).", ge=1)] = 1,
    page_size: Annotated[
        int, Query(description="Registros por página (máx. 200).", ge=1, le=200)
    ] = 20,
) -> PaginationParams:
    """Assemble ``PaginationParams`` from URL query strings.

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
    response_model=KpiContratosMenoresResponse,
    summary="KPIs del módulo Contratos Menores",
    description=(
        "Retorna los indicadores clave del dashboard de contratos menores: "
        "total de contratos, monto acumulado, cantidad completados y en proceso, "
        "porcentaje de avance y número de alertas de fraccionamiento activas. "
        "Acepta filtros opcionales de año, UE y meta."
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
) -> KpiContratosMenoresResponse:
    """Return aggregate KPI figures for the Contratos Menores header cards.

    Args:
        filters: Year, UE, and meta constraints.
        db: Database session injected by ``get_db``.
        _current_user: Authenticated user (validates JWT; not used directly).

    Returns:
        A ``KpiContratosMenoresResponse`` with all six aggregate values.
    """
    logger.debug("GET /contratos-menores/kpis filters=%s", filters)
    return contrato_menor_service.get_kpis(db, filters)


# ---------------------------------------------------------------------------
# GET /graficos
# ---------------------------------------------------------------------------


@router.get(
    "/graficos",
    response_model=list[GraficoContratoMenorItem],
    summary="Gráficos de distribución de contratos menores",
    description=(
        "Retorna dos series de datos para gráficos de distribución: "
        "primero la distribución por estado del proceso, luego por tipo de objeto "
        "(BIEN / SERVICIO / OBRA / CONSULTORIA). "
        "Cada ítem incluye etiqueta, cantidad, monto acumulado y porcentaje. "
        "El frontend distingue las dos series por el valor del campo 'label'."
    ),
    responses={
        200: {"description": "Datos de gráfico calculados exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_graficos(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[GraficoContratoMenorItem]:
    """Return distribution data for minor-contracts charts.

    Args:
        filters: Year, UE, and meta constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        Combined list — status distribution items followed by type distribution items.
    """
    logger.debug("GET /contratos-menores/graficos filters=%s", filters)
    return contrato_menor_service.get_graficos(db, filters)


# ---------------------------------------------------------------------------
# GET /tabla
# ---------------------------------------------------------------------------


@router.get(
    "/tabla",
    response_model=TablaContratosMenoresResponse,
    summary="Tabla paginada de contratos menores",
    description=(
        "Retorna una página de registros de ContratoMenor con todos los labels "
        "resueltos (sigla de UE, código de meta, razón social del proveedor). "
        "Los contratos se ordenan por ID descendente (más recientes primero). "
        "Acepta los mismos filtros que los demás endpoints del módulo."
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
) -> TablaContratosMenoresResponse:
    """Return a paginated minor-contracts detail table with all labels resolved.

    Args:
        filters: Year, UE, and meta constraints.
        pagination: Page number and page size.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``TablaContratosMenoresResponse`` with the current page of rows,
        total row count, and pagination metadata.
    """
    logger.debug(
        "GET /contratos-menores/tabla filters=%s page=%d size=%d",
        filters, pagination.page, pagination.page_size,
    )
    return contrato_menor_service.get_tabla(db, filters, pagination)


# ---------------------------------------------------------------------------
# GET /fraccionamiento
# ---------------------------------------------------------------------------


@router.get(
    "/fraccionamiento",
    response_model=list[FraccionamientoAlerta],
    summary="Alertas de fraccionamiento de contratos menores",
    description=(
        "Detecta y retorna alertas de posible fraccionamiento aplicando dos reglas "
        "del Art. 49.2 de la Ley 32069: "
        "CANTIDAD — 3 o más contratos del mismo DDNNTT + categoría en el mismo mes; "
        "MONTO — acumulado superior a 8 UIT (S/44,000) del mismo DDNNTT + categoría "
        "en el mismo trimestre. "
        "El parámetro 'anio' es obligatorio; 'ue_id' restringe la búsqueda a una UE."
    ),
    responses={
        200: {"description": "Lista de alertas detectadas (puede ser vacía)."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_fraccionamiento(
    anio: Annotated[
        int,
        Query(
            description="Año fiscal para la detección de fraccionamiento.",
            ge=2000,
            le=2100,
        ),
    ],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
    ue_id: Annotated[
        int | None,
        Query(
            description="ID de la UE. Omitir para analizar todas las UEs.",
            ge=1,
        ),
    ] = None,
) -> list[FraccionamientoAlerta]:
    """Run fraccionamiento detection rules for the specified year.

    Args:
        anio: Fiscal year to analyse — required, not optional.
        db: Database session.
        _current_user: Authenticated user guard.
        ue_id: Optional UE restriction.

    Returns:
        List of ``FraccionamientoAlerta`` instances; empty list if none found.
    """
    logger.debug(
        "GET /contratos-menores/fraccionamiento anio=%d ue_id=%s",
        anio, ue_id,
    )
    return contrato_menor_service.detect_fraccionamiento(db, anio=anio, ue_id=ue_id)


# ---------------------------------------------------------------------------
# GET /{id}
# ---------------------------------------------------------------------------


@router.get(
    "/{contrato_id}",
    response_model=ContratoMenorResponse,
    summary="Detalle de un contrato menor con timeline",
    description=(
        "Retorna la ficha completa de un contrato menor incluyendo la lista "
        "ordenada de sus pasos (hitos 1–9) para renderizar el stepper de timeline "
        "en el frontend. Incluye labels resueltos: sigla de UE, código de meta, "
        "razón social del proveedor."
    ),
    responses={
        200: {"description": "Detalle del contrato con procesos."},
        401: {"description": "Token JWT ausente o inválido."},
        404: {"description": "Contrato no encontrado."},
    },
)
def get_detalle(
    contrato_id: int,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> ContratoMenorResponse:
    """Return the full detail of a single minor contract including its stepper timeline.

    Args:
        contrato_id: Primary key of the contract to retrieve.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``ContratoMenorResponse`` with the complete ordered list of procesos.

    Raises:
        HTTPException 404: If no contract with the given ID exists.
    """
    logger.debug("GET /contratos-menores/%d", contrato_id)
    return contrato_menor_service.get_detalle(db, contrato_id)


# ---------------------------------------------------------------------------
# POST /
# ---------------------------------------------------------------------------


@router.post(
    "/",
    response_model=ContratoMenorResponse,
    status_code=201,
    summary="Crear nuevo contrato menor",
    description=(
        "Registra un nuevo proceso de contratación directa ≤8 UIT. "
        "El código se genera automáticamente como 'CM-{año}-{seq:03d}'. "
        "El estado inicial es siempre 'PENDIENTE'. "
        "Requiere rol ADMIN o LOGISTICA."
    ),
    responses={
        201: {"description": "Contrato creado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente (requiere ADMIN o LOGISTICA)."},
        422: {"description": "UE o Meta inexistente, o datos de entrada inválidos."},
    },
)
def create_contrato(
    data: ContratoMenorCreate,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(require_role("ADMIN", "LOGISTICA"))],
) -> ContratoMenorResponse:
    """Create a new minor contracting process.

    Args:
        data: Validated creation payload from the request body.
        db: Database session.
        _current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The newly created ``ContratoMenorResponse`` (HTTP 201).

    Raises:
        HTTPException 422: If ``ue_id`` or ``meta_id`` does not exist.
    """
    logger.info(
        "POST /contratos-menores/ ue_id=%d categoria='%s' monto=%.2f user=%s",
        data.ue_id, data.categoria, data.monto_estimado, _current_user.username,
    )
    contrato: ContratoMenor = contrato_menor_service.create_contrato(db, data)
    return contrato_menor_service.get_detalle(db, contrato.id)


# ---------------------------------------------------------------------------
# PUT /{id}
# ---------------------------------------------------------------------------


@router.put(
    "/{contrato_id}",
    response_model=ContratoMenorResponse,
    summary="Actualizar contrato menor",
    description=(
        "Actualiza parcialmente un contrato menor existente. "
        "Solo los campos incluidos en el cuerpo son modificados. "
        "Permite avanzar el estado, registrar el proveedor adjudicado, "
        "el número de orden y el monto ejecutado. "
        "Requiere rol ADMIN o LOGISTICA."
    ),
    responses={
        200: {"description": "Contrato actualizado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente (requiere ADMIN o LOGISTICA)."},
        404: {"description": "Contrato no encontrado."},
        422: {"description": "Proveedor inexistente o datos de entrada inválidos."},
    },
)
def update_contrato(
    contrato_id: int,
    data: ContratoMenorUpdate,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(require_role("ADMIN", "LOGISTICA"))],
) -> ContratoMenorResponse:
    """Apply a partial update to an existing minor contracting process.

    Args:
        contrato_id: Primary key of the contract to modify.
        data: Validated partial update payload.
        db: Database session.
        _current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The updated ``ContratoMenorResponse``.

    Raises:
        HTTPException 404: Contract not found.
        HTTPException 422: Referenced proveedor does not exist.
    """
    logger.info(
        "PUT /contratos-menores/%d user=%s", contrato_id, _current_user.username
    )
    contrato_menor_service.update_contrato(db, contrato_id, data)
    return contrato_menor_service.get_detalle(db, contrato_id)


# ---------------------------------------------------------------------------
# POST /{id}/procesos
# ---------------------------------------------------------------------------


@router.post(
    "/{contrato_id}/procesos",
    response_model=ContratoMenorProcesoResponse,
    status_code=201,
    summary="Agregar hito al timeline del contrato",
    description=(
        "Agrega un nuevo paso (hito) al stepper de timeline de un contrato menor. "
        "El número de orden (1–9) se asigna automáticamente. "
        "Un contrato solo puede tener un máximo de 9 pasos. "
        "Requiere rol ADMIN o LOGISTICA."
    ),
    responses={
        201: {"description": "Hito creado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente (requiere ADMIN o LOGISTICA)."},
        404: {"description": "Contrato no encontrado."},
        422: {"description": "El contrato ya tiene 9 pasos registrados."},
    },
)
def create_proceso(
    contrato_id: int,
    data: ContratoMenorProcesoCreate,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(require_role("ADMIN", "LOGISTICA"))],
) -> ContratoMenorProcesoResponse:
    """Add a new milestone step to a minor contracting process.

    Args:
        contrato_id: Primary key of the parent contract.
        data: Validated milestone creation payload.
        db: Database session.
        _current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The newly created ``ContratoMenorProcesoResponse`` (HTTP 201).

    Raises:
        HTTPException 404: Parent contract not found.
        HTTPException 422: Contract already at 9-step limit.
    """
    logger.info(
        "POST /contratos-menores/%d/procesos hito='%s' user=%s",
        contrato_id, data.hito, _current_user.username,
    )
    proceso: ContratoMenorProceso = contrato_menor_service.create_proceso(
        db, contrato_id, data
    )
    return ContratoMenorProcesoResponse.model_validate(proceso)


# ---------------------------------------------------------------------------
# PUT /{id}/procesos/{proceso_id}
# ---------------------------------------------------------------------------


@router.put(
    "/{contrato_id}/procesos/{proceso_id}",
    response_model=ContratoMenorProcesoResponse,
    summary="Actualizar hito del timeline",
    description=(
        "Actualiza un paso específico del stepper de timeline de un contrato menor. "
        "Permite registrar la fecha de finalización real y/o cambiar el estado "
        "del hito (COMPLETADO / EN_CURSO / PENDIENTE). "
        "Requiere rol ADMIN o LOGISTICA."
    ),
    responses={
        200: {"description": "Hito actualizado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente (requiere ADMIN o LOGISTICA)."},
        404: {"description": "Paso o contrato no encontrado."},
    },
)
def update_proceso(
    contrato_id: int,
    proceso_id: int,
    data: ContratoMenorProcesoUpdate,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(require_role("ADMIN", "LOGISTICA"))],
) -> ContratoMenorProcesoResponse:
    """Update a single milestone step in a minor contracting process.

    Args:
        contrato_id: Primary key of the parent contract (ownership check).
        proceso_id: Primary key of the milestone step to update.
        data: Validated partial update payload (fecha_fin, estado).
        db: Database session.
        _current_user: Authenticated user with ADMIN or LOGISTICA role.

    Returns:
        The updated ``ContratoMenorProcesoResponse``.

    Raises:
        HTTPException 404: Step not found or belongs to a different contract.
    """
    logger.info(
        "PUT /contratos-menores/%d/procesos/%d user=%s",
        contrato_id, proceso_id, _current_user.username,
    )
    proceso: ContratoMenorProceso = contrato_menor_service.update_proceso(
        db, contrato_id, proceso_id, data
    )
    return ContratoMenorProcesoResponse.model_validate(proceso)
