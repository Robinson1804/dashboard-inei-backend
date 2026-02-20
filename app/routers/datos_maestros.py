"""
Master Data (Datos Maestros) router.

Mounts under ``/api/datos-maestros`` (prefix set in ``main.py``).

These read-only list endpoints exist exclusively to serve the filter
dropdowns in the frontend.  They return only active records and are
intentionally simple: no pagination is applied because the maximum
cardinality of each entity is small (≤ 50 UEs, ≤ 300 metas, etc.).

All endpoints require a valid JWT (``get_current_user``).

Endpoints
---------
GET /unidades-ejecutoras     — All active UnidadEjecutora records.
GET /metas-presupuestales    — Metas filtered by optional ue_id.
GET /actividades-operativas  — AOs filtered by optional meta_id and/or ue_id.
GET /clasificadores          — ClasificadorGasto filtered by optional tipo_generico.
GET /proveedores             — Active Proveedor records.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.actividad_operativa import ActividadOperativa
from app.models.clasificador_gasto import ClasificadorGasto
from app.models.meta_presupuestal import MetaPresupuestal
from app.models.proveedor import Proveedor
from app.models.unidad_ejecutora import UnidadEjecutora
from app.models.usuario import Usuario
from app.schemas.datos_maestros import (
    ActividadOperativaResponse,
    ClasificadorGastoResponse,
    MetaPresupuestalResponse,
    ProveedorResponse,
    UnidadEjecutoraResponse,
)
from app.services.auth_service import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Datos Maestros"])


# ---------------------------------------------------------------------------
# GET /unidades-ejecutoras
# ---------------------------------------------------------------------------


@router.get(
    "/unidades-ejecutoras",
    response_model=list[UnidadEjecutoraResponse],
    summary="Listado de Unidades Ejecutoras",
    description=(
        "Retorna todas las Unidades Ejecutoras activas ordenadas por sigla. "
        "Usado para poblar los selectores de filtro en el frontend."
    ),
    responses={
        200: {"description": "Lista de UEs activas."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def list_unidades_ejecutoras(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[UnidadEjecutoraResponse]:
    """Return all active executing units ordered alphabetically by sigla.

    Args:
        db: Database session injected by ``get_db``.
        _current_user: Authenticated user guard (validates JWT).

    Returns:
        List of ``UnidadEjecutoraResponse`` ordered by ``sigla`` ascending.
    """
    ues = (
        db.query(UnidadEjecutora)
        .filter(UnidadEjecutora.activo.is_(True))
        .order_by(UnidadEjecutora.sigla)
        .all()
    )
    logger.debug("list_unidades_ejecutoras: %d records", len(ues))
    return [UnidadEjecutoraResponse.model_validate(ue) for ue in ues]


# ---------------------------------------------------------------------------
# GET /metas-presupuestales
# ---------------------------------------------------------------------------


@router.get(
    "/metas-presupuestales",
    response_model=list[MetaPresupuestalResponse],
    summary="Listado de Metas Presupuestales",
    description=(
        "Retorna las metas presupuestales activas. Filtrable por ``ue_id`` y ``anio``. "
        "Cada registro incluye la sigla de la UE padre para facilitar la presentación."
    ),
    responses={
        200: {"description": "Lista de metas."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def list_metas_presupuestales(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
    ue_id: Annotated[
        int | None,
        Query(description="Filtrar por ID de Unidad Ejecutora.", ge=1),
    ] = None,
    anio: Annotated[
        int | None,
        Query(description="Filtrar por año fiscal.", ge=2000, le=2100),
    ] = None,
) -> list[MetaPresupuestalResponse]:
    """Return active budget metas, optionally scoped to a UE and/or fiscal year.

    The UE sigla is joined and included in the response so the frontend can
    display it in dropdown labels without a second request.

    Args:
        db: Database session.
        _current_user: Authenticated user guard.
        ue_id: Optional filter by UnidadEjecutora primary key.
        anio: Optional filter by fiscal year.

    Returns:
        List of ``MetaPresupuestalResponse`` ordered by codigo ascending.
    """
    q = (
        db.query(
            MetaPresupuestal.id,
            MetaPresupuestal.codigo,
            MetaPresupuestal.descripcion,
            MetaPresupuestal.sec_funcional,
            MetaPresupuestal.ue_id,
            UnidadEjecutora.sigla.label("ue_sigla"),
            MetaPresupuestal.anio,
            MetaPresupuestal.activo,
        )
        .join(UnidadEjecutora, MetaPresupuestal.ue_id == UnidadEjecutora.id)
        .filter(MetaPresupuestal.activo.is_(True))
    )

    if ue_id is not None:
        q = q.filter(MetaPresupuestal.ue_id == ue_id)
    if anio is not None:
        q = q.filter(MetaPresupuestal.anio == anio)

    rows = q.order_by(MetaPresupuestal.codigo).all()
    logger.debug("list_metas_presupuestales: %d records", len(rows))

    return [
        MetaPresupuestalResponse(
            id=row.id,
            codigo=row.codigo,
            descripcion=row.descripcion,
            sec_funcional=row.sec_funcional,
            ue_id=row.ue_id,
            ue_sigla=row.ue_sigla,
            anio=row.anio,
            activo=row.activo,
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# GET /actividades-operativas
# ---------------------------------------------------------------------------


@router.get(
    "/actividades-operativas",
    response_model=list[ActividadOperativaResponse],
    summary="Listado de Actividades Operativas",
    description=(
        "Retorna las Actividades Operativas activas. "
        "Filtrable opcionalmente por ``meta_id`` y/o ``ue_id``."
    ),
    responses={
        200: {"description": "Lista de AOs."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def list_actividades_operativas(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
    meta_id: Annotated[
        int | None,
        Query(description="Filtrar por ID de Meta Presupuestal.", ge=1),
    ] = None,
    ue_id: Annotated[
        int | None,
        Query(description="Filtrar por ID de Unidad Ejecutora.", ge=1),
    ] = None,
) -> list[ActividadOperativaResponse]:
    """Return active operational activities with optional meta and UE filters.

    Args:
        db: Database session.
        _current_user: Authenticated user guard.
        meta_id: Optional filter by MetaPresupuestal primary key.
        ue_id: Optional filter by UnidadEjecutora primary key.

    Returns:
        List of ``ActividadOperativaResponse`` ordered by codigo_ceplan ascending.
    """
    q = db.query(ActividadOperativa).filter(ActividadOperativa.activo.is_(True))

    if meta_id is not None:
        q = q.filter(ActividadOperativa.meta_id == meta_id)
    if ue_id is not None:
        q = q.filter(ActividadOperativa.ue_id == ue_id)

    aos = q.order_by(ActividadOperativa.codigo_ceplan).all()
    logger.debug("list_actividades_operativas: %d records", len(aos))
    return [ActividadOperativaResponse.model_validate(ao) for ao in aos]


# ---------------------------------------------------------------------------
# GET /clasificadores
# ---------------------------------------------------------------------------


@router.get(
    "/clasificadores",
    response_model=list[ClasificadorGastoResponse],
    summary="Listado de Clasificadores de Gasto",
    description=(
        "Retorna los clasificadores de gasto SIAF. "
        "Filtrable por ``tipo_generico`` (ej. '2.1', '2.3', '2.5', '2.6')."
    ),
    responses={
        200: {"description": "Lista de clasificadores."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def list_clasificadores(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
    tipo_generico: Annotated[
        str | None,
        Query(
            description="Filtrar por tipo genérico: '2.1', '2.3', '2.5' o '2.6'.",
            max_length=10,
        ),
    ] = None,
) -> list[ClasificadorGastoResponse]:
    """Return SIAF expenditure classifiers, optionally filtered by generic type.

    Args:
        db: Database session.
        _current_user: Authenticated user guard.
        tipo_generico: Optional top-level group filter (``"2.1"``, ``"2.3"``,
            ``"2.5"``, or ``"2.6"``).

    Returns:
        List of ``ClasificadorGastoResponse`` ordered by codigo ascending.
    """
    q = db.query(ClasificadorGasto)

    if tipo_generico is not None:
        q = q.filter(ClasificadorGasto.tipo_generico == tipo_generico)

    clasificadores = q.order_by(ClasificadorGasto.codigo).all()
    logger.debug("list_clasificadores: %d records tipo_generico=%s", len(clasificadores), tipo_generico)
    return [ClasificadorGastoResponse.model_validate(c) for c in clasificadores]


# ---------------------------------------------------------------------------
# GET /proveedores
# ---------------------------------------------------------------------------


@router.get(
    "/proveedores",
    response_model=list[ProveedorResponse],
    summary="Listado de Proveedores",
    description=(
        "Retorna los proveedores activos registrados en el sistema, "
        "ordenados por razón social. Usado en los selectores de contrataciones."
    ),
    responses={
        200: {"description": "Lista de proveedores activos."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def list_proveedores(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[ProveedorResponse]:
    """Return all active providers ordered by legal name.

    Args:
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        List of ``ProveedorResponse`` ordered by ``razon_social`` ascending.
    """
    proveedores = (
        db.query(Proveedor)
        .filter(Proveedor.activo.is_(True))
        .order_by(Proveedor.razon_social)
        .all()
    )
    logger.debug("list_proveedores: %d records", len(proveedores))
    return [ProveedorResponse.model_validate(p) for p in proveedores]
