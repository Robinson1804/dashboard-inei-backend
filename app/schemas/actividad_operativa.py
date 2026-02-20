"""
Pydantic v2 schemas for the Actividades Operativas module.

These models define the exact JSON shapes returned by every endpoint in
``app/routers/actividades_operativas.py``.  They mirror the domain of
operational activities (AOs) linked to CEPLAN strategic objectives and
their monthly budget execution tracking.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# KPI summary cards
# ---------------------------------------------------------------------------


class KpiAOResponse(BaseModel):
    """Aggregate KPI figures for the Actividades Operativas Dashboard header.

    Provides total counts broken down by traffic-light semaphore colour,
    plus percentage shares for each colour band.

    Attributes:
        total_aos: Total number of active operational activities.
        verdes: AOs with execution >= 90% (on-track).
        amarillos: AOs with execution 70–89% (at-risk).
        rojos: AOs with execution < 70% (critical).
        porcentaje_verde: Share of green AOs as a percentage (0–100).
        porcentaje_amarillo: Share of yellow AOs as a percentage (0–100).
        porcentaje_rojo: Share of red AOs as a percentage (0–100).
    """

    total_aos: int = Field(..., ge=0, description="Total de Actividades Operativas activas.")
    verdes: int = Field(..., ge=0, description="AOs con ejecución >= 90% (en verde).")
    amarillos: int = Field(..., ge=0, description="AOs con ejecución 70–89% (en amarillo).")
    rojos: int = Field(..., ge=0, description="AOs con ejecución < 70% (en rojo).")
    porcentaje_verde: float = Field(
        ..., ge=0.0, le=100.0, description="Porcentaje de AOs en verde."
    )
    porcentaje_amarillo: float = Field(
        ..., ge=0.0, le=100.0, description="Porcentaje de AOs en amarillo."
    )
    porcentaje_rojo: float = Field(
        ..., ge=0.0, le=100.0, description="Porcentaje de AOs en rojo."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total_aos": 120,
                "verdes": 80,
                "amarillos": 25,
                "rojos": 15,
                "porcentaje_verde": 66.67,
                "porcentaje_amarillo": 20.83,
                "porcentaje_rojo": 12.50,
            }
        }
    )


# ---------------------------------------------------------------------------
# Evolution line chart — monthly programado vs ejecutado
# ---------------------------------------------------------------------------


class GraficoAOEvolucionItem(BaseModel):
    """Monthly evolution data point for the AO programado vs ejecutado chart.

    Aggregates all operational activities' monthly budget values into a
    single series used by the Recharts line chart on the frontend.

    Attributes:
        mes: Spanish month abbreviation shown on the X-axis (e.g. ``"Ene"``).
        programado: Total programmed amount for the month across all AOs.
        ejecutado: Total executed amount for the month across all AOs.
    """

    mes: str = Field(..., description="Etiqueta del mes (ej. 'Ene', 'Feb').")
    programado: float = Field(..., description="Monto programado del mes en soles.")
    ejecutado: float = Field(..., description="Monto ejecutado del mes en soles.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mes": "Jun",
                "programado": 3_200_000.0,
                "ejecutado": 2_950_000.0,
            }
        }
    )


# ---------------------------------------------------------------------------
# Paginated AO summary table
# ---------------------------------------------------------------------------


class AOTablaRow(BaseModel):
    """Single row in the Actividades Operativas summary table.

    Each row represents one AO with aggregated budget totals drawn from
    all its monthly programming records (programacion_mensual).

    Attributes:
        id: ActividadOperativa primary key.
        codigo_ceplan: CEPLAN strategic code, e.g. ``"AOI00000500001"``.
        nombre: Full activity name.
        ue_sigla: Abbreviation of the owning UnidadEjecutora.
        programado_total: Sum of all monthly programado amounts in soles.
        ejecutado_total: Sum of all monthly ejecutado amounts in soles.
        ejecucion_porcentaje: Execution rate (ejecutado / programado × 100).
        semaforo: Traffic-light state — ``"VERDE"``, ``"AMARILLO"``, or ``"ROJO"``.
    """

    id: int = Field(..., description="PK de ActividadOperativa.")
    codigo_ceplan: str = Field(..., description="Código CEPLAN de la actividad.")
    nombre: str = Field(..., description="Nombre completo de la actividad.")
    ue_sigla: str = Field(..., description="Sigla de la Unidad Ejecutora.")
    programado_total: float = Field(..., description="Suma de programado (todos los meses).")
    ejecutado_total: float = Field(..., description="Suma de ejecutado (todos los meses).")
    ejecucion_porcentaje: float = Field(
        ..., ge=0.0, description="Porcentaje de ejecución (ejecutado / programado × 100)."
    )
    semaforo: str = Field(
        ...,
        pattern="^(VERDE|AMARILLO|ROJO)$",
        description="Color del semáforo de ejecución.",
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 7,
                "codigo_ceplan": "AOI00000500001",
                "nombre": "Producción estadística de encuestas de hogares",
                "ue_sigla": "INEI-LIMA",
                "programado_total": 850_000.0,
                "ejecutado_total": 612_000.0,
                "ejecucion_porcentaje": 72.0,
                "semaforo": "AMARILLO",
            }
        },
    )


class AOTablaResponse(BaseModel):
    """Paginated wrapper returned by ``GET /api/actividades-operativas/tabla``.

    Attributes:
        rows: The AO summary rows for the requested page.
        total: Total matching rows ignoring pagination.
        page: Current 1-based page number.
        page_size: Rows per page as requested.
    """

    rows: list[AOTablaRow]
    total: int = Field(..., ge=0, description="Total de registros sin paginar.")
    page: int = Field(..., ge=1, description="Página actual (base 1).")
    page_size: int = Field(..., ge=1, description="Registros por página.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "rows": [],
                "total": 120,
                "page": 1,
                "page_size": 20,
            }
        }
    )


# ---------------------------------------------------------------------------
# Drill-down: AO → classifier level
# ---------------------------------------------------------------------------


class DrillDownTareaItem(BaseModel):
    """One expenditure-classifier breakdown item within an AO drill-down.

    Maps to a single ``ClasificadorGasto`` linked via ``ProgramacionPresupuestal``
    for the selected AO, showing how budget is distributed per classifier.

    Attributes:
        clasificador_codigo: Standard SIAF code, e.g. ``"2.3.1.5.1.2"``.
        clasificador_descripcion: Human-readable classifier name.
        programado: Total programado for this classifier across all months.
        ejecutado: Total ejecutado for this classifier across all months.
        ejecucion_porcentaje: Execution rate for this classifier (0–100+).
    """

    clasificador_codigo: str = Field(
        ..., description="Código del clasificador de gasto (SIAF)."
    )
    clasificador_descripcion: str = Field(
        ..., description="Descripción del clasificador de gasto."
    )
    programado: float = Field(..., description="Monto programado para este clasificador.")
    ejecutado: float = Field(..., description="Monto ejecutado para este clasificador.")
    ejecucion_porcentaje: float = Field(
        ..., ge=0.0, description="Porcentaje de ejecución de este clasificador."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "clasificador_codigo": "2.3.1.5.1.2",
                "clasificador_descripcion": "Pasajes y gastos de transporte",
                "programado": 120_000.0,
                "ejecutado": 95_400.0,
                "ejecucion_porcentaje": 79.5,
            }
        }
    )


class DrillDownAOResponse(BaseModel):
    """Full drill-down view for a single ActividadOperativa.

    Provides the AO header summary plus a list of per-classifier breakdowns
    so that the frontend can render a nested detail panel.

    Attributes:
        ao_id: ActividadOperativa primary key.
        ao_nombre: Full activity name.
        ao_codigo: CEPLAN code of the activity.
        semaforo: Overall traffic-light state for this AO.
        programado_total: Total programado across all classifiers and months.
        ejecutado_total: Total ejecutado across all classifiers and months.
        tareas: Breakdown by expenditure classifier.
    """

    ao_id: int = Field(..., description="PK de ActividadOperativa.")
    ao_nombre: str = Field(..., description="Nombre de la actividad operativa.")
    ao_codigo: str = Field(..., description="Código CEPLAN de la actividad.")
    semaforo: str = Field(
        ...,
        pattern="^(VERDE|AMARILLO|ROJO)$",
        description="Semáforo general de la actividad.",
    )
    programado_total: float = Field(..., description="Total programado de la actividad.")
    ejecutado_total: float = Field(..., description="Total ejecutado de la actividad.")
    tareas: list[DrillDownTareaItem] = Field(
        default_factory=list,
        description="Desglose por clasificador de gasto.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ao_id": 7,
                "ao_nombre": "Producción estadística de encuestas de hogares",
                "ao_codigo": "AOI00000500001",
                "semaforo": "AMARILLO",
                "programado_total": 850_000.0,
                "ejecutado_total": 612_000.0,
                "tareas": [],
            }
        }
    )
