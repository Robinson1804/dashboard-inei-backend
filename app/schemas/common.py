"""
Shared Pydantic v2 schemas reused across multiple modules.

Provides generic filter, pagination, and message response models so that
each domain module can compose them without duplicating field definitions.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class FilterParams(BaseModel):
    """Query-level filters shared across budget, import, and master data endpoints.

    All fields are optional — omitting one means "no restriction on that axis".

    Attributes:
        anio: Fiscal year to filter by (e.g. 2026).
        ue_id: Primary key of a specific UnidadEjecutora.
        meta_id: Primary key of a specific MetaPresupuestal.
        fuente_financiamiento: Funding-source string, e.g. "RO", "RDR".
    """

    anio: int | None = Field(
        default=None,
        ge=2000,
        le=2100,
        description="Año fiscal (ej. 2026). None = todos los años.",
    )
    ue_id: int | None = Field(
        default=None,
        ge=1,
        description="ID de la Unidad Ejecutora. None = todas las UEs.",
    )
    meta_id: int | None = Field(
        default=None,
        ge=1,
        description="ID de la Meta Presupuestal. None = todas las metas.",
    )
    fuente_financiamiento: str | None = Field(
        default=None,
        max_length=100,
        description=(
            "Fuente de financiamiento, ej. 'RO', 'RDR', 'RROO'. "
            "None = todas las fuentes."
        ),
    )


class PaginationParams(BaseModel):
    """Pagination parameters for list endpoints.

    Attributes:
        page: 1-based page number.
        page_size: Number of rows per page (capped at 200 to protect DB).
    """

    page: int = Field(
        default=1,
        ge=1,
        description="Número de página (base 1).",
    )
    page_size: int = Field(
        default=20,
        ge=1,
        le=200,
        description="Registros por página (máximo 200).",
    )


class MessageResponse(BaseModel):
    """Generic message envelope for operations that do not return a resource.

    Returned by write operations (POST, PUT, DELETE) when the caller only
    needs a confirmation, not the full updated resource.

    Attributes:
        message: Short human-readable result summary.
        detail: Optional extended information (error description, hint, etc.).
    """

    message: str = Field(..., description="Resumen del resultado de la operación.")
    detail: str | None = Field(
        default=None,
        description="Información adicional (contexto de error, sugerencia, etc.).",
    )
