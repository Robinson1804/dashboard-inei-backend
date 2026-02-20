"""
Exportación router.

Mounts under ``/api/exportar`` (prefix set in ``main.py``).

All endpoints require a valid JWT token.  Both endpoints stream their
response using FastAPI's ``StreamingResponse`` so that large files are
not buffered in memory beyond the generation step.

Endpoints
---------
GET /excel  — Export to .xlsx (query param: modulo, anio, ue_id, fuente)
GET /pdf    — Export to .pdf  (query param: modulo, anio, ue_id, fuente)

The ``Content-Disposition`` header on each response uses the
``attachment; filename=...`` pattern so that browsers prompt a download
rather than displaying the file inline.

Supported module values for ``?modulo=``
-----------------------------------------
- ``presupuesto``
- ``adquisiciones``
- ``contratos_menores``
- ``actividades_operativas``
"""

from __future__ import annotations

import io
import logging
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.common import FilterParams
from app.services.auth_service import get_current_user
from app.services import exportacion_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Exportación"])

# Module whitelist — validated against this set before querying the DB
_VALID_MODULES = frozenset({
    "presupuesto",
    "adquisiciones",
    "contratos_menores",
    "actividades_operativas",
})


# ---------------------------------------------------------------------------
# Shared dependency — build FilterParams from Query parameters
# ---------------------------------------------------------------------------


def _filter_params(
    anio: Annotated[
        int | None,
        Query(description="Año fiscal, ej. 2026.", ge=2000, le=2100),
    ] = None,
    ue_id: Annotated[
        int | None,
        Query(description="ID de la Unidad Ejecutora.", ge=1),
    ] = None,
    fuente: Annotated[
        str | None,
        Query(
            alias="fuente",
            description="Fuente de financiamiento, ej. 'RO', 'RDR'.",
            max_length=100,
        ),
    ] = None,
) -> FilterParams:
    """Assemble ``FilterParams`` from URL query parameters for export endpoints.

    Args:
        anio: Optional fiscal year.
        ue_id: Optional UnidadEjecutora primary key.
        fuente: Optional funding-source string.

    Returns:
        A validated ``FilterParams`` instance.
    """
    return FilterParams(anio=anio, ue_id=ue_id, fuente_financiamiento=fuente)


def _validate_module(modulo: str) -> str:
    """Validate that the requested module is supported.

    Args:
        modulo: Module key provided by the client.

    Returns:
        The validated module key (lowercased).

    Raises:
        HTTPException 400: If the module is not in the supported set.
    """
    modulo_lower = modulo.lower()
    if modulo_lower not in _VALID_MODULES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Módulo '{modulo}' no soportado. "
                f"Valores válidos: {sorted(_VALID_MODULES)}."
            ),
        )
    return modulo_lower


def _make_filename(modulo: str, ext: str) -> str:
    """Build a safe, timestamped filename for the exported file.

    Args:
        modulo: Module key, used as the filename base.
        ext: File extension without dot, e.g. ``"xlsx"`` or ``"pdf"``.

    Returns:
        Filename string, e.g. ``"inei_presupuesto_2026-02-17.xlsx"``.
    """
    today = date.today().isoformat()
    safe_modulo = modulo.replace(" ", "_").lower()
    return f"inei_{safe_modulo}_{today}.{ext}"


# ---------------------------------------------------------------------------
# GET /excel
# ---------------------------------------------------------------------------


@router.get(
    "/excel",
    summary="Exportar datos a Excel (.xlsx)",
    description=(
        "Genera y descarga un archivo Excel (.xlsx) con los datos del módulo indicado. "
        "Incluye cabecera institucional INEI, sección de KPIs y tabla de datos formateada. "
        "Requiere autenticación."
    ),
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Archivo Excel generado exitosamente.",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            },
        },
        400: {"description": "Módulo no válido."},
        401: {"description": "Token JWT ausente o inválido."},
        500: {"description": "Error generando el archivo."},
    },
)
def export_excel(
    modulo: Annotated[
        str,
        Query(
            description=(
                "Módulo a exportar: presupuesto, adquisiciones, "
                "contratos_menores, actividades_operativas."
            ),
        ),
    ],
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> StreamingResponse:
    """Generate and stream an Excel file for the specified dashboard module.

    Args:
        modulo: Module to export data from.
        filters: Year, UE, and funding-source constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``StreamingResponse`` with the ``.xlsx`` file attached.

    Raises:
        HTTPException 400: If the module name is invalid.
        HTTPException 500: If Excel generation fails unexpectedly.
    """
    modulo_key = _validate_module(modulo)
    logger.info(
        "GET /exportar/excel modulo=%s anio=%s ue_id=%s",
        modulo_key, filters.anio, filters.ue_id,
    )

    try:
        file_bytes = exportacion_service.export_excel(db, modulo_key, filters)
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dependencia faltante para exportación Excel: {exc}",
        ) from exc
    except Exception as exc:
        logger.exception("export_excel failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generando el archivo Excel: {exc}",
        ) from exc

    filename = _make_filename(modulo_key, "xlsx")
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(len(file_bytes)),
    }

    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# ---------------------------------------------------------------------------
# GET /pdf
# ---------------------------------------------------------------------------


@router.get(
    "/pdf",
    summary="Exportar datos a PDF (.pdf)",
    description=(
        "Genera y descarga un archivo PDF con los datos del módulo indicado. "
        "Incluye cabecera institucional INEI, resumen de KPIs, tabla de datos y "
        "pie de página con numeración. Requiere autenticación."
    ),
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Archivo PDF generado exitosamente.",
            "content": {"application/pdf": {}},
        },
        400: {"description": "Módulo no válido."},
        401: {"description": "Token JWT ausente o inválido."},
        500: {"description": "Error generando el archivo."},
    },
)
def export_pdf(
    modulo: Annotated[
        str,
        Query(
            description=(
                "Módulo a exportar: presupuesto, adquisiciones, "
                "contratos_menores, actividades_operativas."
            ),
        ),
    ],
    filters: Annotated[FilterParams, Depends(_filter_params)],
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> StreamingResponse:
    """Generate and stream a PDF file for the specified dashboard module.

    Args:
        modulo: Module to export data from.
        filters: Year, UE, and funding-source constraints.
        db: Database session.
        _current_user: Authenticated user guard.

    Returns:
        A ``StreamingResponse`` with the ``.pdf`` file attached.

    Raises:
        HTTPException 400: If the module name is invalid.
        HTTPException 500: If PDF generation fails unexpectedly.
    """
    modulo_key = _validate_module(modulo)
    logger.info(
        "GET /exportar/pdf modulo=%s anio=%s ue_id=%s",
        modulo_key, filters.anio, filters.ue_id,
    )

    try:
        file_bytes = exportacion_service.export_pdf(db, modulo_key, filters)
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Dependencia faltante para exportación PDF: {exc}",
        ) from exc
    except Exception as exc:
        logger.exception("export_pdf failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generando el archivo PDF: {exc}",
        ) from exc

    filename = _make_filename(modulo_key, "pdf")
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(len(file_bytes)),
    }

    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type="application/pdf",
        headers=headers,
    )
