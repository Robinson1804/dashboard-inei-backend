"""
Pydantic v2 schemas for the Master Data (Datos Maestros) module.

These read-only response schemas are used by ``GET`` list endpoints that
power the filter dropdowns in the frontend.  All models enable ORM mode
(``from_attributes=True``) so that SQLAlchemy model instances can be
serialised directly without manual conversion.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# UnidadEjecutora
# ---------------------------------------------------------------------------


class UnidadEjecutoraResponse(BaseModel):
    """Public representation of an executing unit (DDNNTT).

    Attributes:
        id: Primary key.
        codigo: Short internal code, e.g. ``"001"``.
        nombre: Full legal name.
        sigla: Abbreviation shown in the UI, e.g. ``"INEI-LIMA"``.
        tipo: Classification — ``"CENTRAL"`` or ``"ODEI"``.
        activo: Whether the unit is active (soft-delete flag).
    """

    id: int
    codigo: str
    nombre: str
    sigla: str
    tipo: str | None = None
    activo: bool

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "codigo": "001",
                "nombre": "Instituto Nacional de Estadística e Informática - Lima",
                "sigla": "INEI-LIMA",
                "tipo": "CENTRAL",
                "activo": True,
            }
        },
    )


# ---------------------------------------------------------------------------
# MetaPresupuestal
# ---------------------------------------------------------------------------


class MetaPresupuestalResponse(BaseModel):
    """Budget meta / target linked to an executing unit.

    Attributes:
        id: Primary key.
        codigo: Meta code, e.g. ``"0001"``.
        descripcion: Human-readable description of the meta.
        sec_funcional: Functional sequence code from SIAF.
        ue_id: Foreign key to UnidadEjecutora.
        ue_sigla: Sigla of the parent UnidadEjecutora (joined for convenience).
        anio: Fiscal year this meta belongs to.
        activo: Soft-delete flag.
    """

    id: int
    codigo: str
    descripcion: str | None = None
    sec_funcional: str | None = None
    ue_id: int
    ue_sigla: str = Field(..., description="Sigla de la UE (join).")
    anio: int
    activo: bool

    model_config = ConfigDict(
        from_attributes=False,  # populated from query tuples, not ORM objects
        json_schema_extra={
            "example": {
                "id": 7,
                "codigo": "0042",
                "descripcion": "Producción estadística y difusión de información",
                "sec_funcional": "5000042",
                "ue_id": 1,
                "ue_sigla": "INEI-LIMA",
                "anio": 2026,
                "activo": True,
            }
        },
    )


# ---------------------------------------------------------------------------
# ActividadOperativa
# ---------------------------------------------------------------------------


class ActividadOperativaResponse(BaseModel):
    """Operational activity (AO) used in budget planning and tracking.

    Attributes:
        id: Primary key.
        codigo_ceplan: CEPLAN code, e.g. ``"AOI00000500001"``.
        nombre: Full activity name.
        oei: Institutional Strategic Objective.
        aei: Institutional Strategic Action.
        meta_id: FK to MetaPresupuestal (nullable).
        ue_id: FK to UnidadEjecutora (nullable).
        anio: Fiscal year.
        activo: Soft-delete flag.
    """

    id: int
    codigo_ceplan: str
    nombre: str
    oei: str | None = None
    aei: str | None = None
    meta_id: int | None = None
    ue_id: int | None = None
    anio: int | None = None
    activo: bool

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 3,
                "codigo_ceplan": "AOI00000500001",
                "nombre": "Producción y difusión de estadísticas nacionales",
                "oei": "OEI 1: Estadísticas confiables y oportunas",
                "aei": "AEI 1.1: Censos y encuestas nacionales",
                "meta_id": 7,
                "ue_id": 1,
                "anio": 2026,
                "activo": True,
            }
        },
    )


# ---------------------------------------------------------------------------
# ClasificadorGasto
# ---------------------------------------------------------------------------


class ClasificadorGastoResponse(BaseModel):
    """Expenditure classifier (SIAF standard coding scheme).

    Attributes:
        id: Primary key.
        codigo: Unique classifier code, e.g. ``"2.3.1.5.1.2"``.
        descripcion: Full expenditure type name.
        tipo_generico: Top-level group — ``"2.1"``, ``"2.3"``, ``"2.5"``, or ``"2.6"``.
    """

    id: int
    codigo: str
    descripcion: str
    tipo_generico: str | None = None

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 12,
                "codigo": "2.3.1.5.1.2",
                "descripcion": "Pasajes y gastos de transporte",
                "tipo_generico": "2.3",
            }
        },
    )


# ---------------------------------------------------------------------------
# Proveedor
# ---------------------------------------------------------------------------


class ProveedorResponse(BaseModel):
    """Supplier / vendor record used in acquisitions and minor contracts.

    Attributes:
        id: Primary key.
        ruc: Tax ID number (11 digits).
        razon_social: Legal company name.
        nombre_comercial: Trade name (optional).
        estado_rnp: National Providers Registry status.
        activo: Soft-delete flag.
    """

    id: int
    ruc: str
    razon_social: str
    nombre_comercial: str | None = None
    estado_rnp: str | None = None
    activo: bool

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 5,
                "ruc": "20123456789",
                "razon_social": "CONSULTORES ESTADÍSTICOS SAC",
                "nombre_comercial": "CONSULT-STAT",
                "estado_rnp": "HABIDO",
                "activo": True,
            }
        },
    )
