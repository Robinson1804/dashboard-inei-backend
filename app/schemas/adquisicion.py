"""
Pydantic v2 schemas for the Adquisiciones >8 UIT module.

These models define the exact JSON shapes consumed and returned by every
endpoint in ``app/routers/adquisiciones.py``.  They are deliberately free of
SQLAlchemy imports so that the schema layer stays decoupled from ORM internals.

Domain context
--------------
Adquisiciones >8 UIT follow the full 22-step OSCE procurement workflow
(Ley 32069) divided across three phases:

1. Actuaciones Preparatorias  — internal preparation, expedient approval.
2. Selección                  — SEACE publication, committee evaluation.
3. Ejecución Contractual      — contract signature, delivery, payment.

Each acquisition has:
- A main ``Adquisicion`` record (header + status).
- One optional ``AdquisicionDetalle`` record (SEACE / PLADICOP references).
- Between 1 and 22 ``AdquisicionProceso`` milestone records (Gantt timeline).
"""

from __future__ import annotations

import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Input schemas — write operations
# ---------------------------------------------------------------------------


class AdquisicionCreate(BaseModel):
    """Payload for creating a new procurement process (POST /).

    The ``codigo`` field is optional: when omitted the service auto-generates
    a value in the format ``ADQ-{anio}-{seq:03d}`` (e.g. ``ADQ-2026-001``).

    Attributes:
        descripcion: Full description of the object being procured (1-1000 chars).
        tipo_objeto: Category — one of ``BIEN``, ``SERVICIO``, ``OBRA``,
                     ``CONSULTORIA``.
        tipo_procedimiento: OSCE procedure class — one of the values defined in
                            ``constants.TIPOS_PROCEDIMIENTO``.
        ue_id: Primary key of the responsible UnidadEjecutora.
        meta_id: Primary key of the associated MetaPresupuestal.
        monto_referencial: Reference value in soles before adjudication.
        codigo: Optional explicit process code.  Generated automatically when
                omitted.
    """

    descripcion: str = Field(
        ...,
        min_length=5,
        max_length=1000,
        description="Descripción del objeto de la contratación.",
    )
    tipo_objeto: str = Field(
        ...,
        description="Tipo de objeto: BIEN, SERVICIO, OBRA o CONSULTORIA.",
    )
    tipo_procedimiento: str = Field(
        ...,
        description=(
            "Tipo de procedimiento OSCE: LICITACION_PUBLICA, CONCURSO_PUBLICO, "
            "SUBASTA_INVERSA, COMPARACION_PRECIOS, CONTRATACION_DIRECTA, "
            "CATALOGO_ELECTRONICO o DIALOGO_COMPETITIVO."
        ),
    )
    ue_id: int = Field(..., ge=1, description="ID de la Unidad Ejecutora.")
    meta_id: int = Field(..., ge=1, description="ID de la Meta Presupuestal.")
    monto_referencial: float = Field(
        ...,
        gt=0,
        description="Monto referencial en soles (debe superar las 8 UIT).",
    )
    codigo: str | None = Field(
        default=None,
        max_length=20,
        description=(
            "Código del proceso, ej. 'ADQ-2026-001'. "
            "Se genera automáticamente si se omite."
        ),
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "descripcion": "Adquisición de equipos de cómputo portátiles",
                "tipo_objeto": "BIEN",
                "tipo_procedimiento": "LICITACION_PUBLICA",
                "ue_id": 1,
                "meta_id": 3,
                "monto_referencial": 250000.00,
                "codigo": None,
            }
        }
    )


class AdquisicionUpdate(BaseModel):
    """Payload for partial update of an existing procurement process (PUT /{id}).

    All fields are optional so callers can PATCH only the attributes they want
    to change without touching the rest of the record.

    Attributes:
        descripcion: Updated description.
        tipo_objeto: Updated object type.
        tipo_procedimiento: Updated OSCE procedure class.
        ue_id: Updated executing unit.
        meta_id: Updated budget meta.
        monto_referencial: Updated reference amount.
        monto_adjudicado: Awarded contract value (set after adjudication).
        estado: Updated process state from ``constants.ESTADOS_ADQUISICION``.
        fase_actual: Updated phase from ``constants.FASES_ADQUISICION``.
        proveedor_id: FK to the awarded Proveedor (set after adjudication).
    """

    descripcion: str | None = Field(
        default=None,
        min_length=5,
        max_length=1000,
        description="Descripción actualizada del objeto de la contratación.",
    )
    tipo_objeto: str | None = Field(
        default=None,
        description="Tipo de objeto actualizado.",
    )
    tipo_procedimiento: str | None = Field(
        default=None,
        description="Tipo de procedimiento OSCE actualizado.",
    )
    ue_id: int | None = Field(
        default=None,
        ge=1,
        description="ID actualizado de la Unidad Ejecutora.",
    )
    meta_id: int | None = Field(
        default=None,
        ge=1,
        description="ID actualizado de la Meta Presupuestal.",
    )
    monto_referencial: float | None = Field(
        default=None,
        gt=0,
        description="Monto referencial actualizado en soles.",
    )
    monto_adjudicado: float | None = Field(
        default=None,
        gt=0,
        description="Monto adjudicado en soles (se registra al adjudicar).",
    )
    estado: str | None = Field(
        default=None,
        description=(
            "Estado del proceso: EN_ACTOS_PREPARATORIOS, EN_SELECCION, "
            "EN_EJECUCION, ADJUDICADO, CULMINADO, DESIERTO o NULO."
        ),
    )
    fase_actual: str | None = Field(
        default=None,
        description=(
            "Fase actual: ACTUACIONES_PREPARATORIAS, SELECCION "
            "o EJECUCION_CONTRACTUAL."
        ),
    )
    proveedor_id: int | None = Field(
        default=None,
        ge=1,
        description="ID del Proveedor adjudicado.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "estado": "ADJUDICADO",
                "fase_actual": "EJECUCION_CONTRACTUAL",
                "monto_adjudicado": 238500.00,
                "proveedor_id": 7,
            }
        }
    )


# ---------------------------------------------------------------------------
# Response schemas — read operations
# ---------------------------------------------------------------------------


class AdquisicionResponse(BaseModel):
    """Full procurement process record returned by list and detail endpoints.

    Denormalised fields (``ue_sigla``, ``meta_codigo``, ``proveedor_razon_social``)
    are resolved server-side by joining the related tables so that the frontend
    does not need extra requests.

    Attributes:
        id: Primary key.
        codigo: Unique process code, e.g. ``ADQ-2026-001``.
        anio: Fiscal year.
        ue_id: FK to UnidadEjecutora.
        ue_sigla: Resolved UE abbreviation (e.g. ``INEI-LIMA``).
        meta_id: FK to MetaPresupuestal.
        meta_codigo: Resolved meta code (e.g. ``0001``).
        descripcion: Object description.
        tipo_objeto: Object type.
        tipo_procedimiento: OSCE procedure type.
        estado: Current process state.
        fase_actual: Current phase.
        monto_referencial: Reference amount in soles.
        monto_adjudicado: Awarded amount in soles (None until adjudicated).
        proveedor_id: FK to awarded Proveedor (None until adjudicated).
        proveedor_razon_social: Resolved vendor legal name (None until adjudicated).
        created_at: Record creation timestamp.
        updated_at: Last modification timestamp.
    """

    id: int
    codigo: str = Field(..., description="Código único del proceso, ej. 'ADQ-2026-001'.")
    anio: int | None = Field(default=None, description="Año fiscal.")
    ue_id: int | None = Field(default=None, description="ID de la Unidad Ejecutora.")
    ue_sigla: str | None = Field(default=None, description="Sigla de la Unidad Ejecutora.")
    meta_id: int | None = Field(default=None, description="ID de la Meta Presupuestal.")
    meta_codigo: str | None = Field(default=None, description="Código de la Meta Presupuestal.")
    descripcion: str = Field(..., description="Descripción del objeto de la contratación.")
    tipo_objeto: str | None = Field(default=None, description="Tipo de objeto.")
    tipo_procedimiento: str | None = Field(default=None, description="Tipo de procedimiento OSCE.")
    estado: str | None = Field(default=None, description="Estado actual del proceso.")
    fase_actual: str | None = Field(default=None, description="Fase actual del proceso.")
    monto_referencial: float | None = Field(
        default=None, description="Monto referencial en soles."
    )
    monto_adjudicado: float | None = Field(
        default=None, description="Monto adjudicado en soles."
    )
    proveedor_id: int | None = Field(default=None, description="ID del proveedor adjudicado.")
    proveedor_razon_social: str | None = Field(
        default=None, description="Razón social del proveedor adjudicado."
    )
    created_at: datetime.datetime = Field(..., description="Fecha de creación del registro.")
    updated_at: datetime.datetime = Field(
        ..., description="Fecha de última modificación."
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "codigo": "ADQ-2026-001",
                "anio": 2026,
                "ue_id": 1,
                "ue_sigla": "INEI-LIMA",
                "meta_id": 3,
                "meta_codigo": "0003",
                "descripcion": "Adquisición de equipos de cómputo portátiles",
                "tipo_objeto": "BIEN",
                "tipo_procedimiento": "LICITACION_PUBLICA",
                "estado": "EN_SELECCION",
                "fase_actual": "SELECCION",
                "monto_referencial": 250000.00,
                "monto_adjudicado": None,
                "proveedor_id": None,
                "proveedor_razon_social": None,
                "created_at": "2026-01-15T08:00:00",
                "updated_at": "2026-02-01T14:30:00",
            }
        },
    )


class AdquisicionDetalleResponse(BaseModel):
    """Extended procurement details (1:1 with Adquisicion).

    Attributes:
        id: Primary key of the AdquisicionDetalle record.
        adquisicion_id: FK to the parent Adquisicion.
        n_expediente: Internal expedient number.
        n_proceso_seace: SEACE portal process number.
        n_proceso_pladicop: PLADICOP process number.
        bases_url: Public URL of the published procurement bases.
        resolucion_aprobacion: Resolution number approving the expedient.
        fecha_aprobacion_expediente: Date the expedient was approved.
        observaciones: Free-text observations.
    """

    id: int
    adquisicion_id: int
    n_expediente: str | None = Field(default=None, description="Número de expediente interno.")
    n_proceso_seace: str | None = Field(
        default=None, description="Número de proceso en SEACE."
    )
    n_proceso_pladicop: str | None = Field(
        default=None, description="Número de proceso en PLADICOP."
    )
    bases_url: str | None = Field(
        default=None, description="URL de las bases publicadas en SEACE."
    )
    resolucion_aprobacion: str | None = Field(
        default=None, description="Número de resolución que aprueba el expediente."
    )
    fecha_aprobacion_expediente: datetime.date | None = Field(
        default=None, description="Fecha de aprobación del expediente."
    )
    observaciones: str | None = Field(
        default=None, description="Observaciones generales del proceso."
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "adquisicion_id": 1,
                "n_expediente": "EXP-2026-0042",
                "n_proceso_seace": "SEACE-LP-2026-001",
                "n_proceso_pladicop": None,
                "bases_url": "https://seace.gob.pe/bases/lp-2026-001",
                "resolucion_aprobacion": "RES-DEC-001-2026",
                "fecha_aprobacion_expediente": "2026-01-20",
                "observaciones": None,
            }
        },
    )


# ---------------------------------------------------------------------------
# Proceso (milestone) schemas
# ---------------------------------------------------------------------------


class AdquisicionProcesoCreate(BaseModel):
    """Payload for adding a milestone to the Gantt timeline (POST /{id}/procesos).

    Attributes:
        orden: Sequential position within the 22-step workflow (1–22).
        hito: Milestone name, e.g. ``"Aprobación de expediente de contratación"``.
        fase: Phase this milestone belongs to.
        area_responsable: Area responsible for completing the milestone.
        dias_planificados: Planned duration in working days.
        fecha_inicio: Planned start date.
        estado: Initial milestone status — typically ``PENDIENTE``.
    """

    orden: int = Field(
        ...,
        ge=1,
        le=22,
        description="Posición secuencial del hito (1–22).",
    )
    hito: str = Field(
        ...,
        min_length=3,
        max_length=200,
        description="Nombre del hito del proceso.",
    )
    fase: str | None = Field(
        default=None,
        description=(
            "Fase del hito: ACTUACIONES_PREPARATORIAS, SELECCION "
            "o EJECUCION_CONTRACTUAL."
        ),
    )
    area_responsable: str | None = Field(
        default=None,
        description="Área responsable: OTIN, DEC, OTA, OTPP, PROVEEDOR o COMITÉ.",
    )
    dias_planificados: int | None = Field(
        default=None,
        ge=1,
        description="Duración planificada en días hábiles.",
    )
    fecha_inicio: datetime.date | None = Field(
        default=None,
        description="Fecha de inicio planificada.",
    )
    estado: str = Field(
        default="PENDIENTE",
        description="Estado inicial del hito: COMPLETADO, EN_CURSO, PENDIENTE u OBSERVADO.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "orden": 1,
                "hito": "Aprobación de expediente de contratación",
                "fase": "ACTUACIONES_PREPARATORIAS",
                "area_responsable": "OTIN",
                "dias_planificados": 10,
                "fecha_inicio": "2026-01-15",
                "estado": "PENDIENTE",
            }
        }
    )


class AdquisicionProcesoUpdate(BaseModel):
    """Payload for updating a milestone status/dates (PUT /{id}/procesos/{proceso_id}).

    All fields are optional — callers send only the attributes that changed.

    Attributes:
        fecha_fin: Updated planned end date.
        fecha_real_inicio: Actual start date (set when the milestone begins).
        fecha_real_fin: Actual end date (set when the milestone is completed).
        estado: Updated milestone status.
        observacion: Free-text note about progress or blocking issues.
    """

    fecha_fin: datetime.date | None = Field(
        default=None,
        description="Fecha de fin planificada actualizada.",
    )
    fecha_real_inicio: datetime.date | None = Field(
        default=None,
        description="Fecha real de inicio del hito.",
    )
    fecha_real_fin: datetime.date | None = Field(
        default=None,
        description="Fecha real de fin del hito.",
    )
    estado: str | None = Field(
        default=None,
        description="Estado actualizado: COMPLETADO, EN_CURSO, PENDIENTE u OBSERVADO.",
    )
    observacion: str | None = Field(
        default=None,
        description="Observación libre sobre el avance o bloqueos del hito.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fecha_real_inicio": "2026-01-16",
                "fecha_real_fin": "2026-01-28",
                "estado": "COMPLETADO",
                "observacion": "Aprobado sin observaciones por la DEC.",
            }
        }
    )


class AdquisicionProcesoResponse(BaseModel):
    """Full milestone record returned from the timeline endpoints.

    Attributes:
        id: Primary key.
        adquisicion_id: FK to the parent Adquisicion.
        orden: Sequential position (1–22).
        hito: Milestone name.
        fase: Phase this milestone belongs to.
        area_responsable: Responsible area abbreviation.
        dias_planificados: Planned duration in working days.
        fecha_inicio: Planned start date.
        fecha_fin: Planned end date.
        fecha_real_inicio: Actual start date (None if not yet started).
        fecha_real_fin: Actual end date (None if not yet completed).
        estado: Current milestone status.
        observacion: Free-text observations.
    """

    id: int
    adquisicion_id: int
    orden: int = Field(..., description="Posición secuencial del hito (1–22).")
    hito: str = Field(..., description="Nombre del hito.")
    fase: str | None = Field(default=None, description="Fase del hito.")
    area_responsable: str | None = Field(default=None, description="Área responsable.")
    dias_planificados: int | None = Field(
        default=None, description="Duración planificada en días hábiles."
    )
    fecha_inicio: datetime.date | None = Field(
        default=None, description="Fecha de inicio planificada."
    )
    fecha_fin: datetime.date | None = Field(
        default=None, description="Fecha de fin planificada."
    )
    fecha_real_inicio: datetime.date | None = Field(
        default=None, description="Fecha real de inicio."
    )
    fecha_real_fin: datetime.date | None = Field(
        default=None, description="Fecha real de fin."
    )
    estado: str | None = Field(default=None, description="Estado del hito.")
    observacion: str | None = Field(default=None, description="Observación del hito.")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "adquisicion_id": 1,
                "orden": 1,
                "hito": "Aprobación de expediente de contratación",
                "fase": "ACTUACIONES_PREPARATORIAS",
                "area_responsable": "OTIN",
                "dias_planificados": 10,
                "fecha_inicio": "2026-01-15",
                "fecha_fin": "2026-01-28",
                "fecha_real_inicio": "2026-01-16",
                "fecha_real_fin": "2026-01-28",
                "estado": "COMPLETADO",
                "observacion": None,
            }
        },
    )


# ---------------------------------------------------------------------------
# KPI and chart aggregation schemas
# ---------------------------------------------------------------------------


class KpiAdquisicionesResponse(BaseModel):
    """Aggregate KPI figures for the Adquisiciones >8 UIT dashboard header.

    Attributes:
        total: Total number of procurement processes matching the current filters.
        monto_pim: Sum of monto_referencial for all matching processes (in soles).
        monto_adjudicado: Sum of monto_adjudicado for adjudicated processes (in soles).
        avance_porcentaje: Percentage of processes in ADJUDICADO/CULMINADO state
                           relative to the total.
        culminados: Count of processes in CULMINADO state.
        en_proceso: Count of processes in any non-terminal state
                    (EN_ACTOS_PREPARATORIOS, EN_SELECCION, EN_EJECUCION).
        by_estado: Distribution of process count keyed by estado value.
    """

    total: int = Field(..., ge=0, description="Total de adquisiciones.")
    monto_pim: float = Field(
        ..., description="Suma de montos referenciales en soles."
    )
    monto_adjudicado: float = Field(
        ..., description="Suma de montos adjudicados en soles."
    )
    avance_porcentaje: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description=(
            "Porcentaje de procesos en estado ADJUDICADO o CULMINADO "
            "sobre el total."
        ),
    )
    culminados: int = Field(
        ..., ge=0, description="Cantidad de procesos en estado CULMINADO."
    )
    en_proceso: int = Field(
        ...,
        ge=0,
        description=(
            "Cantidad de procesos en estados activos: EN_ACTOS_PREPARATORIOS, "
            "EN_SELECCION o EN_EJECUCION."
        ),
    )
    by_estado: dict[str, int] = Field(
        ...,
        description=(
            "Distribución de procesos por estado. "
            "Claves: valores de constants.ESTADOS_ADQUISICION."
        ),
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total": 28,
                "monto_pim": 4_500_000.0,
                "monto_adjudicado": 1_800_000.0,
                "avance_porcentaje": 35.71,
                "culminados": 5,
                "en_proceso": 18,
                "by_estado": {
                    "EN_ACTOS_PREPARATORIOS": 6,
                    "EN_SELECCION": 8,
                    "EN_EJECUCION": 4,
                    "ADJUDICADO": 5,
                    "CULMINADO": 5,
                    "DESIERTO": 0,
                    "NULO": 0,
                },
            }
        }
    )


class GraficoAdquisicionItem(BaseModel):
    """Single slice of the pie/donut chart showing procurement distribution.

    Used by ``GET /graficos`` to render the estado distribution chart on the
    Adquisiciones dashboard.

    Attributes:
        estado: Estado code, e.g. ``EN_SELECCION``.
        label: Human-readable Spanish label for the estado.
        cantidad: Number of processes in this state.
        porcentaje: Percentage of total processes this state represents (0–100).
        monto: Sum of monto_referencial for processes in this state (in soles).
    """

    estado: str = Field(..., description="Código del estado, ej. 'EN_SELECCION'.")
    label: str = Field(
        ..., description="Etiqueta legible del estado para mostrar en el gráfico."
    )
    cantidad: int = Field(..., ge=0, description="Cantidad de procesos en este estado.")
    porcentaje: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Porcentaje del total de procesos que representa este estado.",
    )
    monto: float = Field(
        ..., description="Suma de montos referenciales en soles para este estado."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "estado": "EN_SELECCION",
                "label": "En Selección",
                "cantidad": 8,
                "porcentaje": 28.57,
                "monto": 1_200_000.0,
            }
        }
    )


# ---------------------------------------------------------------------------
# Full detail response (header + detalle + timeline)
# ---------------------------------------------------------------------------


class AdquisicionDetalleFullResponse(BaseModel):
    """Complete procurement record: header, extended detail, and full timeline.

    Returned by ``GET /{id}`` to populate the Gantt modal / detail panel on the
    frontend.  Combines data from three tables to avoid multiple round-trips.

    Attributes:
        adquisicion: The main Adquisicion header record.
        detalle: The 1:1 extended detail record (None if not yet created).
        procesos: Ordered list of milestone records (up to 22) for the Gantt
                  timeline.
    """

    adquisicion: AdquisicionResponse
    detalle: AdquisicionDetalleResponse | None = Field(
        default=None,
        description="Detalle extendido del proceso (SEACE, PLADICOP, etc.).",
    )
    procesos: list[AdquisicionProcesoResponse] = Field(
        default_factory=list,
        description="Hitos del cronograma Gantt ordenados por campo 'orden' (1–22).",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "adquisicion": {
                    "id": 1,
                    "codigo": "ADQ-2026-001",
                    "anio": 2026,
                    "ue_id": 1,
                    "ue_sigla": "INEI-LIMA",
                    "meta_id": 3,
                    "meta_codigo": "0003",
                    "descripcion": "Adquisición de equipos de cómputo portátiles",
                    "tipo_objeto": "BIEN",
                    "tipo_procedimiento": "LICITACION_PUBLICA",
                    "estado": "EN_SELECCION",
                    "fase_actual": "SELECCION",
                    "monto_referencial": 250000.00,
                    "monto_adjudicado": None,
                    "proveedor_id": None,
                    "proveedor_razon_social": None,
                    "created_at": "2026-01-15T08:00:00",
                    "updated_at": "2026-02-01T14:30:00",
                },
                "detalle": None,
                "procesos": [],
            }
        }
    )


# ---------------------------------------------------------------------------
# Paginated table response
# ---------------------------------------------------------------------------


class TablaAdquisicionesResponse(BaseModel):
    """Paginated list of procurement processes returned by ``GET /tabla``.

    Attributes:
        rows: Current page of ``AdquisicionResponse`` objects.
        total: Total number of matching rows (before pagination).
        page: Current page number (1-based).
        page_size: Number of rows per page as requested.
    """

    rows: list[AdquisicionResponse] = Field(
        ..., description="Registros de la página actual."
    )
    total: int = Field(..., ge=0, description="Total de registros sin paginar.")
    page: int = Field(..., ge=1, description="Página actual (base 1).")
    page_size: int = Field(..., ge=1, description="Registros por página.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "rows": [],
                "total": 28,
                "page": 1,
                "page_size": 20,
            }
        }
    )


# ---------------------------------------------------------------------------
# Filter extension for the adquisiciones module
# ---------------------------------------------------------------------------


class AdquisicionFilterParams(BaseModel):
    """Extended filter parameters specific to the Adquisiciones module.

    Extends the base ``FilterParams`` concept with procurement-specific axes.

    Attributes:
        anio: Fiscal year.
        ue_id: Executing unit primary key.
        meta_id: Budget meta primary key.
        estado: Process state code from ``constants.ESTADOS_ADQUISICION``.
        tipo_procedimiento: OSCE procedure type.
        fase: Current phase from ``constants.FASES_ADQUISICION``.
    """

    anio: int | None = Field(
        default=None,
        ge=2000,
        le=2100,
        description="Año fiscal.",
    )
    ue_id: int | None = Field(
        default=None,
        ge=1,
        description="ID de la Unidad Ejecutora.",
    )
    meta_id: int | None = Field(
        default=None,
        ge=1,
        description="ID de la Meta Presupuestal.",
    )
    estado: str | None = Field(
        default=None,
        description="Estado del proceso de adquisición.",
    )
    tipo_procedimiento: str | None = Field(
        default=None,
        description="Tipo de procedimiento OSCE.",
    )
    fase: str | None = Field(
        default=None,
        description="Fase actual del proceso.",
    )
