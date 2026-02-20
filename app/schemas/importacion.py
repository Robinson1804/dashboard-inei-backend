"""
Pydantic v2 schemas for the Import (Importacion) module.

Covers:
- Upload response after file processing.
- Historical record for GET /api/importacion/historial.
- Format catalog for GET /api/importacion/formatos-catalogo.
- Format status dashboard for GET /api/importacion/estado-formatos.
- Master-data response shapes.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Upload result
# ---------------------------------------------------------------------------


class ImportacionUploadResponse(BaseModel):
    """Summary returned after processing an uploaded Excel or SIAF/SIGA file."""

    formato_detectado: str = Field(
        ...,
        description="Etiqueta del formato detectado (ej. 'FORMATO_1', 'SIAF', 'SIGA').",
    )
    registros_validos: int = Field(..., ge=0)
    registros_error: int = Field(..., ge=0)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "formato_detectado": "FORMATO_2",
                "registros_validos": 312,
                "registros_error": 4,
                "warnings": ["Fila 15: valor PIM negativo reemplazado por 0."],
                "errors": ["Fila 78: clasificador '9.9.9' no existe en la base de datos."],
                "metadata": {
                    "archivo": "Formato2_INEI_2026.xlsx",
                    "ue_detectada": "INEI-LIMA",
                    "anio": 2026,
                    "total_filas_leidas": 316,
                },
            }
        }
    )


# ---------------------------------------------------------------------------
# Import history record
# ---------------------------------------------------------------------------


class HistorialImportacion(BaseModel):
    """Single row in the import history list."""

    id: int = Field(..., description="PK del registro de historial.")
    formato: str
    archivo_nombre: str
    fecha: datetime
    usuario: str
    ue: str | None = None
    registros_ok: int = Field(..., ge=0)
    registros_error: int = Field(..., ge=0)
    estado: str

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Format catalog
# ---------------------------------------------------------------------------


class FormatoCatalogItem(BaseModel):
    """Single entry in the available formats catalog."""

    key: str
    nombre: str
    descripcion: str
    hoja: str
    columnas: int
    fila_inicio: int
    tiene_plantilla: bool = False

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "key": "formato1",
                "nombre": "Formato 1 - Programacion Presupuestal",
                "descripcion": "Programacion anual con desglose mensual.",
                "hoja": "Formato 1",
                "columnas": 17,
                "fila_inicio": 8,
                "tiene_plantilla": True,
            }
        }
    )


# ---------------------------------------------------------------------------
# Format status dashboard (C1)
# ---------------------------------------------------------------------------


class FormatoEstadoItem(BaseModel):
    """Status of a single import format in the dashboard."""

    formato: str = Field(..., description="Clave del formato (ej. 'FORMATO_1', 'SIAF').")
    plantilla_key: str = Field(..., description="Key para descargar plantilla (ej. 'formato1').")
    nombre: str = Field(..., description="Nombre legible del formato.")
    descripcion: str = Field(..., description="Descripción breve del contenido.")
    categoria: str = Field(
        ...,
        description="Categoría: 'DATOS_MAESTROS', 'FORMATOS_DDNNTT', 'SISTEMAS_EXTERNOS'.",
    )
    es_requerido: bool = Field(..., description="Si es obligatorio para el funcionamiento del sistema.")
    tiene_plantilla: bool = Field(..., description="Si existe plantilla descargable.")
    impacto: str = Field(..., description="Qué dashboards o funcionalidades alimenta.")
    upload_endpoint: str = Field(..., description="Endpoint para subir este formato.")
    ultima_carga: str | None = Field(None, description="ISO timestamp de la última carga exitosa.")
    estado: str = Field(
        ...,
        description="Estado: 'SIN_CARGAR', 'EXITOSO', 'PARCIAL', 'FALLIDO'.",
    )
    registros_ok: int = Field(0, ge=0, description="Registros de la última carga exitosa.")
    usuario_ultima_carga: str | None = Field(None, description="Usuario que realizó la última carga.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "formato": "FORMATO_1",
                "nombre": "Formato 1 - Programación Presupuestal",
                "descripcion": "Programación anual con desglose mensual",
                "categoria": "FORMATOS_DDNNTT",
                "es_requerido": True,
                "tiene_plantilla": True,
                "impacto": "Dashboard Presupuesto: KPIs PIA/PIM/Certificado/Devengado",
                "upload_endpoint": "/api/importacion/formatos",
                "ultima_carga": "2026-02-15T14:30:00+00:00",
                "estado": "EXITOSO",
                "registros_ok": 312,
                "usuario_ultima_carga": "admin",
            }
        }
    )


class EstadoFormatosResponse(BaseModel):
    """Complete format status dashboard response."""

    formatos: list[FormatoEstadoItem] = Field(
        ..., description="Lista de 12 formatos con su estado de carga."
    )
    total: int = Field(..., description="Total de formatos en el catálogo.")
    cargados_exitosos: int = Field(..., ge=0, description="Formatos con última carga exitosa.")
    cargados_parcial: int = Field(..., ge=0, description="Formatos con última carga parcial.")
    sin_cargar: int = Field(..., ge=0, description="Formatos que nunca se han cargado.")
    requeridos_faltantes: int = Field(
        ..., ge=0, description="Formatos requeridos que no se han cargado."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "formatos": [],
                "total": 12,
                "cargados_exitosos": 5,
                "cargados_parcial": 1,
                "sin_cargar": 6,
                "requeridos_faltantes": 3,
            }
        }
    )
