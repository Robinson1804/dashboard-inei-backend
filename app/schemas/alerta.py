"""
Pydantic v2 schemas for the Alertas module.

These models define the JSON shapes for all endpoints under
``/api/alertas``.  Alerts are generated automatically by the alert engine
(``alerta_service.generar_alertas``) whenever business-rule thresholds
are breached across any of the four dashboard modules.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Full alert resource
# ---------------------------------------------------------------------------


class AlertaResponse(BaseModel):
    """Full representation of a single system alert.

    Mirrors the ``Alerta`` ORM model and resolves the ``ue_id`` foreign key
    to the human-readable ``ue_sigla`` via a service-layer join.

    Attributes:
        id: Alerta primary key.
        tipo: Alert rule identifier (e.g. ``"SUB_EJECUCION_AO"``).
        nivel: Severity level — ``"ROJO"``, ``"AMARILLO"``, or ``"VERDE"``.
        titulo: Short, human-readable title for the alert card.
        descripcion: Detailed description including context and recommended action.
        ue_sigla: Abbreviation of the associated UnidadEjecutora (may be None).
        modulo: Dashboard module that generated the alert.
        entidad_id: Polymorphic ID of the source entity.
        entidad_tipo: Type name of the source entity (e.g. ``"actividad_operativa"``).
        leida: Whether the alert has been read.
        resuelta: Whether the alert has been resolved.
        fecha_generacion: Timestamp when the alert was generated.
    """

    id: int = Field(..., description="PK de la alerta.")
    tipo: str | None = Field(None, description="Tipo/regla de alerta.")
    nivel: str | None = Field(None, description="Nivel de severidad: ROJO, AMARILLO, VERDE.")
    titulo: str | None = Field(None, description="Título corto de la alerta.")
    descripcion: str | None = Field(None, description="Descripción detallada de la alerta.")
    ue_sigla: str | None = Field(None, description="Sigla de la Unidad Ejecutora afectada.")
    modulo: str | None = Field(None, description="Módulo del dashboard que generó la alerta.")
    entidad_id: int | None = Field(None, description="ID de la entidad fuente (polimórfico).")
    entidad_tipo: str | None = Field(None, description="Tipo de la entidad fuente.")
    leida: bool = Field(..., description="Si la alerta ya fue leída.")
    resuelta: bool = Field(..., description="Si la alerta ya fue resuelta.")
    fecha_generacion: datetime = Field(..., description="Fecha y hora de generación.")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "tipo": "SUB_EJECUCION_AO",
                "nivel": "ROJO",
                "titulo": "Sub-ejecución crítica en AO",
                "descripcion": (
                    "La actividad 'Encuestas de hogares' (INEI-LIMA) presenta "
                    "una ejecución del 45.3%, por debajo del umbral mínimo del 70%."
                ),
                "ue_sigla": "INEI-LIMA",
                "modulo": "ACTIVIDADES_OPERATIVAS",
                "entidad_id": 7,
                "entidad_tipo": "actividad_operativa",
                "leida": False,
                "resuelta": False,
                "fecha_generacion": "2026-02-17T08:30:00",
            }
        },
    )


# ---------------------------------------------------------------------------
# Summary / counter response
# ---------------------------------------------------------------------------


class AlertaResumenResponse(BaseModel):
    """Aggregate alert counts for the dashboard notification badge.

    Provides the totals needed to render the alert bell icon with unread
    count, severity breakdown, and per-module distribution.

    Attributes:
        total: Total number of alerts matching the current filter scope.
        no_leidas: Alerts that have not yet been read.
        rojas: Alerts at ROJO (critical) severity.
        amarillas: Alerts at AMARILLO (warning) severity.
        by_modulo: Map of module name → alert count for that module.
    """

    total: int = Field(..., ge=0, description="Total de alertas.")
    no_leidas: int = Field(..., ge=0, description="Alertas no leídas.")
    rojas: int = Field(..., ge=0, description="Alertas de nivel ROJO.")
    amarillas: int = Field(..., ge=0, description="Alertas de nivel AMARILLO.")
    by_modulo: dict[str, int] = Field(
        default_factory=dict,
        description="Conteo de alertas por módulo del dashboard.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total": 34,
                "no_leidas": 12,
                "rojas": 8,
                "amarillas": 18,
                "by_modulo": {
                    "PRESUPUESTO": 5,
                    "ADQUISICIONES": 9,
                    "CONTRATOS_MENORES": 10,
                    "ACTIVIDADES_OPERATIVAS": 10,
                },
            }
        }
    )


# ---------------------------------------------------------------------------
# Update request
# ---------------------------------------------------------------------------


class AlertaUpdateRequest(BaseModel):
    """Partial update payload for marking alerts as read or resolved.

    Both fields are optional so that callers may update only ``leida``,
    only ``resuelta``, or both in a single request.

    Attributes:
        leida: Set to ``True`` to mark the alert as read.
        resuelta: Set to ``True`` to mark the alert as resolved.
    """

    leida: bool | None = Field(
        default=None, description="Marcar la alerta como leída (True)."
    )
    resuelta: bool | None = Field(
        default=None, description="Marcar la alerta como resuelta (True)."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "leida": True,
                "resuelta": False,
            }
        }
    )


# ---------------------------------------------------------------------------
# Alert engine result
# ---------------------------------------------------------------------------


class GenerarAlertasResponse(BaseModel):
    """Response returned after running the alert generation engine.

    Attributes:
        alertas_generadas: Number of new Alerta records inserted.
        anio: The fiscal year that was evaluated.
        mensaje: Human-readable summary of the generation run.
    """

    alertas_generadas: int = Field(
        ..., ge=0, description="Cantidad de alertas nuevas generadas."
    )
    anio: int = Field(..., description="Año fiscal evaluado.")
    mensaje: str = Field(..., description="Resumen del resultado de la generación.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "alertas_generadas": 7,
                "anio": 2026,
                "mensaje": "Motor de alertas ejecutado. 7 alertas nuevas generadas para el año 2026.",
            }
        }
    )
