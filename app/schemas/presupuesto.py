"""
Pydantic v2 schemas for the Budget Dashboard (Presupuesto) module.

These models define the exact JSON shapes returned by every endpoint in
``app/routers/presupuesto.py``.  They are deliberately free of SQLAlchemy
imports so that the schema layer stays decoupled from ORM internals.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# KPI summary card
# ---------------------------------------------------------------------------


class KpiPresupuestoResponse(BaseModel):
    """Aggregate figures displayed in the KPI cards at the top of the
    Budget Dashboard.

    Attributes:
        total_ues: Number of distinct UnidadEjecutora with budget records.
        total_metas: Number of distinct MetaPresupuestal with budget records.
        pim_total: Sum of PIM (Modified Institutional Budget) in soles.
        certificado_total: Sum of certificado amounts in soles.
        comprometido_total: Sum of compromiso_anual amounts in soles.
        devengado_total: Sum of devengado (accrued expenditure) in soles.
        ejecucion_porcentaje: Devengado / PIM as a percentage (0–100).
    """

    total_ues: int = Field(..., ge=0, description="Cantidad de Unidades Ejecutoras con presupuesto.")
    total_metas: int = Field(..., ge=0, description="Cantidad de Metas Presupuestales con presupuesto.")
    pim_total: float = Field(..., description="Suma del PIM en soles.")
    certificado_total: float = Field(..., description="Suma del monto certificado en soles.")
    comprometido_total: float = Field(..., description="Suma del compromiso anual en soles.")
    devengado_total: float = Field(..., description="Suma del devengado en soles.")
    ejecucion_porcentaje: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Porcentaje de ejecución: devengado / PIM × 100.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total_ues": 42,
                "total_metas": 187,
                "pim_total": 245_000_000.0,
                "certificado_total": 198_500_000.0,
                "comprometido_total": 185_000_000.0,
                "devengado_total": 175_320_000.0,
                "ejecucion_porcentaje": 71.56,
            }
        }
    )


# ---------------------------------------------------------------------------
# Bar chart — PIM vs Certificado vs Devengado by UE
# ---------------------------------------------------------------------------


class GraficoBarItem(BaseModel):
    """Single bar-chart data point grouping three budget amounts for one entity.

    Used for:
    - ``GET /grafico-pim-certificado``: PIM vs Certificado by UE.
    - ``GET /grafico-ejecucion``: top UEs ranked by execution %.

    Attributes:
        nombre: Label for the X-axis (UE sigla or name).
        pim: PIM amount in soles.
        certificado: Certified amount in soles.
        devengado: Accrued expenditure in soles.
        ejecucion_porcentaje: devengado / pim × 100, rounded to 2 dp.
    """

    nombre: str = Field(..., description="Etiqueta del eje X (sigla o nombre de la UE).")
    pim: float = Field(..., description="PIM en soles.")
    certificado: float = Field(..., description="Monto certificado en soles.")
    devengado: float = Field(..., description="Devengado en soles.")
    ejecucion_porcentaje: float = Field(
        default=0.0,
        ge=0.0,
        description="Porcentaje de ejecución (devengado / PIM × 100).",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "nombre": "INEI-LIMA",
                "pim": 12_500_000.0,
                "certificado": 10_800_000.0,
                "devengado": 9_400_000.0,
                "ejecucion_porcentaje": 75.2,
            }
        }
    )


# ---------------------------------------------------------------------------
# Evolution line chart — monthly programado vs ejecutado
# ---------------------------------------------------------------------------


class GraficoEvolucionItem(BaseModel):
    """Monthly evolution data point for the programado vs ejecutado line chart.

    Attributes:
        mes: Month label shown on the X-axis (e.g. ``"Ene"``, ``"Feb"``).
        programado: Sum of programado for the month in soles.
        ejecutado: Sum of ejecutado for the month in soles.
    """

    mes: str = Field(..., description="Etiqueta del mes (ej. 'Ene', 'Feb').")
    programado: float = Field(..., description="Monto programado del mes en soles.")
    ejecutado: float = Field(..., description="Monto ejecutado del mes en soles.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mes": "Mar",
                "programado": 8_500_000.0,
                "ejecutado": 7_920_000.0,
            }
        }
    )


# ---------------------------------------------------------------------------
# Paginated detail table
# ---------------------------------------------------------------------------


class TablaPresupuestoRow(BaseModel):
    """Single row in the paginated budget detail table.

    Each row represents one ProgramacionPresupuestal record joined to its
    parent UnidadEjecutora, MetaPresupuestal, and ClasificadorGasto.

    Attributes:
        id: ProgramacionPresupuestal primary key.
        ue: UnidadEjecutora sigla (short code).
        meta: MetaPresupuestal codigo.
        clasificador: ClasificadorGasto codigo, e.g. ``"2.3.1.5.1.2"``.
        descripcion: ClasificadorGasto descripcion.
        pim: PIM in soles.
        certificado: Certified amount in soles.
        devengado: Accrued expenditure in soles.
        saldo: Available balance (PIM - devengado) in soles.
        ejecucion: Execution percentage (0–100).
    """

    id: int
    ue: str = Field(..., description="Sigla de la Unidad Ejecutora.")
    meta: str = Field(..., description="Código de la Meta Presupuestal.")
    clasificador: str = Field(..., description="Código del clasificador de gasto.")
    descripcion: str = Field(..., description="Descripción del clasificador de gasto.")
    pim: float
    certificado: float
    devengado: float
    saldo: float
    ejecucion: float = Field(..., ge=0.0, description="Porcentaje de ejecución (0–100).")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "ue": "INEI-LIMA",
                "meta": "0001",
                "clasificador": "2.3.1.5.1.2",
                "descripcion": "Pasajes y gastos de transporte",
                "pim": 150_000.0,
                "certificado": 130_000.0,
                "devengado": 110_000.0,
                "saldo": 40_000.0,
                "ejecucion": 73.33,
            }
        },
    )


class TablaPresupuestoResponse(BaseModel):
    """Paginated wrapper returned by ``GET /api/presupuesto/tabla``.

    Attributes:
        rows: The budget rows for the requested page.
        total: Total number of matching rows (ignoring pagination).
        page: Current page number (1-based).
        page_size: Number of rows per page as requested.
    """

    rows: list[TablaPresupuestoRow]
    total: int = Field(..., ge=0, description="Total de registros sin paginar.")
    page: int = Field(..., ge=1, description="Página actual (base 1).")
    page_size: int = Field(..., ge=1, description="Registros por página.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "rows": [],
                "total": 450,
                "page": 1,
                "page_size": 20,
            }
        }
    )
