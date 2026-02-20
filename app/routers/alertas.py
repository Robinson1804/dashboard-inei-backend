"""
Alertas router.

Mounts under ``/api/alertas`` (prefix set in ``main.py``).

All endpoints require a valid JWT token.  The ``POST /generar`` endpoint
additionally requires the ``ADMIN`` role as it triggers a potentially
long-running database operation.

Endpoints
---------
GET  /           — List alerts with optional filters (?leida=false&modulo=PRESUPUESTO).
GET  /resumen    — Aggregate counts for the notification badge.
PUT  /{id}/leer  — Mark a specific alert as read.
PUT  /{id}/resolver — Mark a specific alert as resolved.
POST /generar    — Run the alert engine for a fiscal year (ADMIN only).
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.alerta import (
    AlertaResumenResponse,
    AlertaResponse,
    GenerarAlertasResponse,
)
from app.schemas.common import FilterParams
from app.services.auth_service import get_current_user, require_role
from app.services import alerta_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Alertas"])


# ---------------------------------------------------------------------------
# Shared dependency — build FilterParams from Query parameters
# ---------------------------------------------------------------------------


def _filter_params(
    ue_id: Annotated[
        int | None,
        Query(description="ID de la Unidad Ejecutora. Omitir para todas las UEs.", ge=1),
    ] = None,
) -> FilterParams:
    """Assemble ``FilterParams`` for alert list endpoints.

    Alerts are not filtered by fiscal year directly; the ``anio`` field is
    not applicable here.  Only ``ue_id`` is exposed as an alert-level filter.

    Args:
        ue_id: Optional UnidadEjecutora primary key.

    Returns:
        A validated ``FilterParams`` instance with only ``ue_id`` set.
    """
    return FilterParams(ue_id=ue_id)


# ---------------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------------


@router.get(
    "/",
    response_model=list[AlertaResponse],
    summary="Listar alertas del sistema",
    description=(
        "Retorna todas las alertas ordenadas por fecha de generación descendente. "
        "Permite filtrar por unidad ejecutora, estado de lectura y módulo del dashboard."
    ),
    responses={
        200: {"description": "Lista de alertas."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_alertas(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    leida: Annotated[
        bool | None,
        Query(description="Filtrar por estado de lectura. True = leídas, False = no leídas."),
    ] = None,
    modulo: Annotated[
        str | None,
        Query(
            description=(
                "Módulo generador: PRESUPUESTO, ADQUISICIONES, "
                "CONTRATOS_MENORES, ACTIVIDADES_OPERATIVAS."
            ),
            max_length=50,
        ),
    ] = None,
    db: Session = Depends(get_db),
    _current_user: Annotated[Usuario, Depends(get_current_user)] = None,
) -> list[AlertaResponse]:
    """Return alerts matching the given filter criteria.

    Args:
        filters: UE constraint.
        leida: Optional filter by read status (True/False).
        modulo: Optional filter by originating dashboard module.
        db: Database session injected by ``get_db``.
        _current_user: Authenticated user guard.

    Returns:
        List of ``AlertaResponse`` instances, newest first.
    """
    logger.debug(
        "GET /alertas/ ue_id=%s leida=%s modulo=%s",
        filters.ue_id, leida, modulo,
    )
    return alerta_service.get_alertas(db, filters, leida=leida, modulo=modulo)


# ---------------------------------------------------------------------------
# GET /resumen
# ---------------------------------------------------------------------------


@router.get(
    "/resumen",
    response_model=AlertaResumenResponse,
    summary="Resumen de alertas (conteos)",
    description=(
        "Retorna el total de alertas, cuántas no han sido leídas, cuántas son "
        "ROJO y AMARILLO, y un desglose por módulo del dashboard."
    ),
    responses={
        200: {"description": "Resumen calculado exitosamente."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_resumen(
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> AlertaResumenResponse:
    """Return aggregate alert counts for the notification badge.

    Args:
        filters: UE constraint.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        An ``AlertaResumenResponse`` with total, unread, and severity counts.
    """
    logger.debug("GET /alertas/resumen ue_id=%s", filters.ue_id)
    return alerta_service.get_resumen(db, filters)


# ---------------------------------------------------------------------------
# PUT /{id}/leer
# ---------------------------------------------------------------------------


@router.put(
    "/{alerta_id}/leer",
    response_model=AlertaResponse,
    summary="Marcar alerta como leída",
    description="Actualiza el estado de lectura de una alerta específica y registra la fecha de lectura.",
    responses={
        200: {"description": "Alerta marcada como leída."},
        401: {"description": "Token JWT ausente o inválido."},
        404: {"description": "Alerta no encontrada."},
    },
)
def marcar_leida(
    alerta_id: Annotated[int, Path(description="ID de la alerta a marcar como leída.", ge=1)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> AlertaResponse:
    """Mark a specific alert as read.

    Args:
        alerta_id: Primary key of the alert.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        The updated ``AlertaResponse`` with ``leida=True``.

    Raises:
        HTTPException 404: If the alert does not exist.
    """
    logger.debug("PUT /alertas/%d/leer", alerta_id)
    alerta = alerta_service.marcar_leida(db, alerta_id)

    # Resolve ue_sigla for the response
    ue_sigla: str | None = None
    if alerta.unidad_ejecutora is not None:
        ue_sigla = alerta.unidad_ejecutora.sigla

    return AlertaResponse(
        id=alerta.id,
        tipo=alerta.tipo,
        nivel=alerta.nivel,
        titulo=alerta.titulo,
        descripcion=alerta.descripcion,
        ue_sigla=ue_sigla,
        modulo=alerta.modulo,
        entidad_id=alerta.entidad_id,
        entidad_tipo=alerta.entidad_tipo,
        leida=alerta.leida,
        resuelta=alerta.resuelta,
        fecha_generacion=alerta.fecha_generacion,
    )


# ---------------------------------------------------------------------------
# PUT /{id}/resolver
# ---------------------------------------------------------------------------


@router.put(
    "/{alerta_id}/resolver",
    response_model=AlertaResponse,
    summary="Marcar alerta como resuelta",
    description=(
        "Actualiza el estado de resolución de una alerta y registra la fecha de resolución. "
        "Las alertas resueltas no serán consideradas duplicadas por el motor de alertas."
    ),
    responses={
        200: {"description": "Alerta marcada como resuelta."},
        401: {"description": "Token JWT ausente o inválido."},
        404: {"description": "Alerta no encontrada."},
    },
)
def marcar_resuelta(
    alerta_id: Annotated[int, Path(description="ID de la alerta a marcar como resuelta.", ge=1)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> AlertaResponse:
    """Mark a specific alert as resolved.

    Args:
        alerta_id: Primary key of the alert.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        The updated ``AlertaResponse`` with ``resuelta=True``.

    Raises:
        HTTPException 404: If the alert does not exist.
    """
    logger.debug("PUT /alertas/%d/resolver", alerta_id)
    alerta = alerta_service.marcar_resuelta(db, alerta_id)

    ue_sigla: str | None = None
    if alerta.unidad_ejecutora is not None:
        ue_sigla = alerta.unidad_ejecutora.sigla

    return AlertaResponse(
        id=alerta.id,
        tipo=alerta.tipo,
        nivel=alerta.nivel,
        titulo=alerta.titulo,
        descripcion=alerta.descripcion,
        ue_sigla=ue_sigla,
        modulo=alerta.modulo,
        entidad_id=alerta.entidad_id,
        entidad_tipo=alerta.entidad_tipo,
        leida=alerta.leida,
        resuelta=alerta.resuelta,
        fecha_generacion=alerta.fecha_generacion,
    )


# ---------------------------------------------------------------------------
# POST /generar
# ---------------------------------------------------------------------------


@router.post(
    "/generar",
    response_model=GenerarAlertasResponse,
    summary="Ejecutar motor de generación de alertas (ADMIN)",
    description=(
        "Evalúa las 8 reglas de negocio contra el estado actual de la base de datos "
        "para el año fiscal indicado e inserta registros de Alerta para cada umbral "
        "superado. Requiere rol ADMIN. "
        "La operación es idempotente: no duplica alertas no resueltas existentes."
    ),
    responses={
        200: {"description": "Motor ejecutado. Número de nuevas alertas retornado."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Acceso denegado. Se requiere rol ADMIN."},
    },
)
def generar_alertas(
    anio: Annotated[
        int,
        Query(
            description="Año fiscal a evaluar, ej. 2026.",
            ge=2000,
            le=2100,
        ),
    ] = 2026,
    db: Annotated[Session, Depends(get_db)] = None,
    _admin_user: Annotated[Usuario, Depends(require_role("ADMIN"))] = None,
) -> GenerarAlertasResponse:
    """Trigger the alert engine for the specified fiscal year.

    Evaluates all 8 business rules and inserts ``Alerta`` records for any
    threshold breaches discovered.  Uses ``require_role("ADMIN")`` so that
    only administrators can trigger this potentially expensive operation.

    Args:
        anio: Fiscal year to evaluate.
        db: Database session.
        _admin_user: Admin-role user guard (validates JWT + ADMIN role).

    Returns:
        A ``GenerarAlertasResponse`` with the count of new alerts generated.
    """
    logger.info("POST /alertas/generar anio=%d", anio)
    nuevas = alerta_service.generar_alertas(db, anio)
    return GenerarAlertasResponse(
        alertas_generadas=nuevas,
        anio=anio,
        mensaje=(
            f"Motor de alertas ejecutado. {nuevas} alerta(s) nueva(s) "
            f"generadas para el año {anio}."
        ),
    )
