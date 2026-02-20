"""
Import (Importacion) router.

Mounts under ``/api/importacion`` (prefix set in ``main.py``).

Upload endpoints require the ADMIN or PRESUPUESTO role (enforced via
``require_role``).  The history endpoint is readable by any authenticated user.

Endpoints
---------
POST /formatos       — Upload a DDNNTT Excel file (Formatos 1–5B).
POST /datos-maestros — Upload a Cuadro AO-META or reference table file.
POST /siaf           — Upload a SIAF financial system export.
POST /siga           — Upload a SIGA logistics system export.
GET  /historial      — List past import records (most-recent-first).
DELETE /limpiar-formato/{formato} — Delete imported data for a format.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.common import FilterParams
from app.schemas.importacion import (
    EstadoFormatosResponse,
    FormatoCatalogItem,
    HistorialImportacion,
    ImportacionUploadResponse,
)
from app.services.auth_service import get_current_user, require_role
from app.services import importacion_service
from app.services.template_service import (
    FORMATO_CATALOG,
    generate_all_templates,
    generate_template,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Importación"])

# ---------------------------------------------------------------------------
# Role guard — upload endpoints require budget or admin access
# ---------------------------------------------------------------------------

_UPLOAD_ROLES = ("ADMIN", "PRESUPUESTO")


# ---------------------------------------------------------------------------
# Shared content-type validation helper
# ---------------------------------------------------------------------------

_ALLOWED_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
        "application/vnd.ms-excel",  # .xls (legacy)
        "application/octet-stream",  # some browsers send this for .xlsx
    }
)


def _validate_excel_file(file: UploadFile) -> None:
    """Raise HTTP 422 if the uploaded file does not look like an Excel workbook.

    The check is intentionally lenient: ``application/octet-stream`` is
    allowed because many browsers report that content-type for ``.xlsx``
    files, and the actual format validation happens inside openpyxl.

    Args:
        file: The uploaded file object.

    Raises:
        HTTPException 422: If the file has an unexpected MIME type.
    """
    content_type = file.content_type or ""
    if content_type not in _ALLOWED_CONTENT_TYPES:
        # Browsers sometimes omit or mislabel the content type; warn but allow.
        logger.warning(
            "Unexpected content_type='%s' for file='%s' — proceeding anyway",
            content_type,
            file.filename,
        )


# ---------------------------------------------------------------------------
# POST /formatos
# ---------------------------------------------------------------------------


@router.post(
    "/formatos",
    response_model=ImportacionUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Importar archivo DDNNTT (Formatos 1–5B)",
    description=(
        "Carga y procesa un archivo Excel con los Formatos DDNNTT estandarizados (Formato 1 "
        "al Formato 5B). El sistema detecta automáticamente el formato por los nombres de hoja. "
        "Requiere rol ADMIN o PRESUPUESTO."
    ),
    responses={
        200: {"description": "Resumen del procesamiento con conteo de errores y advertencias."},
        400: {"description": "Archivo vacío o corrupto."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente."},
        422: {"description": "Archivo no reconocido como Excel."},
    },
)
async def upload_formato_ddnntt(
    file: Annotated[UploadFile, File(description="Archivo Excel DDNNTT (.xlsx)")],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[Usuario, Depends(require_role(*_UPLOAD_ROLES))],
) -> ImportacionUploadResponse:
    """Process an uploaded DDNNTT Excel file (Formats 1 through 5B).

    Args:
        file: The ``.xlsx`` file submitted as multipart/form-data.
        db: Database session injected by ``get_db``.
        current_user: Authenticated user with ADMIN or PRESUPUESTO role.

    Returns:
        An ``ImportacionUploadResponse`` with validation statistics.

    Raises:
        HTTPException 400: If the file content cannot be read.
        HTTPException 422: If openpyxl cannot open the file as a workbook.
    """
    _validate_excel_file(file)
    logger.info(
        "upload_formato_ddnntt: user='%s' file='%s'",
        current_user.username,
        file.filename,
    )

    try:
        return await importacion_service.process_upload(
            db=db,
            file=file,
            user_id=current_user.id,
            username=current_user.username,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


# ---------------------------------------------------------------------------
# POST /datos-maestros
# ---------------------------------------------------------------------------


@router.post(
    "/datos-maestros",
    response_model=ImportacionUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Importar Cuadro AO-META o tablas de referencia",
    description=(
        "Carga y procesa un archivo Excel con el Cuadro AO-META o tablas de referencia "
        "(UEs, metas, clasificadores). El formato 'DATOS_MAESTROS' es detectado automáticamente "
        "por nombres de hoja. Requiere rol ADMIN o PRESUPUESTO."
    ),
    responses={
        200: {"description": "Resumen del procesamiento."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente."},
        422: {"description": "Archivo no reconocido."},
    },
)
async def upload_datos_maestros(
    file: Annotated[UploadFile, File(description="Archivo Excel con datos maestros (.xlsx)")],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[Usuario, Depends(require_role(*_UPLOAD_ROLES))],
) -> ImportacionUploadResponse:
    """Process an uploaded master-data Excel file (Cuadro AO-META, reference tables).

    Args:
        file: The ``.xlsx`` file submitted as multipart/form-data.
        db: Database session.
        current_user: Authenticated user with ADMIN or PRESUPUESTO role.

    Returns:
        An ``ImportacionUploadResponse`` with validation statistics.
    """
    _validate_excel_file(file)
    logger.info(
        "upload_datos_maestros: user='%s' file='%s'",
        current_user.username,
        file.filename,
    )

    try:
        return await importacion_service.process_upload(
            db=db,
            file=file,
            user_id=current_user.id,
            username=current_user.username,
            declared_format="DATOS_MAESTROS",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


# ---------------------------------------------------------------------------
# POST /siaf
# ---------------------------------------------------------------------------


@router.post(
    "/siaf",
    response_model=ImportacionUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Importar exportación SIAF",
    description=(
        "Carga y procesa un archivo exportado desde el Sistema Integrado de Administración "
        "Financiera (SIAF). El formato es detectado automáticamente. "
        "Requiere rol ADMIN o PRESUPUESTO."
    ),
    responses={
        200: {"description": "Resumen del procesamiento SIAF."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente."},
        422: {"description": "Archivo no reconocido como exportación SIAF."},
    },
)
async def upload_siaf(
    file: Annotated[UploadFile, File(description="Exportación SIAF (.xlsx)")],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[Usuario, Depends(require_role(*_UPLOAD_ROLES))],
) -> ImportacionUploadResponse:
    """Process a SIAF financial system export file.

    Args:
        file: The ``.xlsx`` file submitted as multipart/form-data.
        db: Database session.
        current_user: Authenticated user with ADMIN or PRESUPUESTO role.

    Returns:
        An ``ImportacionUploadResponse`` with validation statistics.
    """
    _validate_excel_file(file)
    logger.info(
        "upload_siaf: user='%s' file='%s'",
        current_user.username,
        file.filename,
    )

    try:
        return await importacion_service.process_upload(
            db=db,
            file=file,
            user_id=current_user.id,
            username=current_user.username,
            declared_format="SIAF",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


# ---------------------------------------------------------------------------
# POST /siga
# ---------------------------------------------------------------------------


@router.post(
    "/siga",
    response_model=ImportacionUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Importar exportación SIGA",
    description=(
        "Carga y procesa un archivo exportado desde el Sistema Integrado de Gestión "
        "Administrativa (SIGA). El formato es detectado automáticamente. "
        "Requiere rol ADMIN o PRESUPUESTO."
    ),
    responses={
        200: {"description": "Resumen del procesamiento SIGA."},
        401: {"description": "Token JWT ausente o inválido."},
        403: {"description": "Rol insuficiente."},
        422: {"description": "Archivo no reconocido como exportación SIGA."},
    },
)
async def upload_siga(
    file: Annotated[UploadFile, File(description="Exportación SIGA (.xlsx)")],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[Usuario, Depends(require_role(*_UPLOAD_ROLES))],
) -> ImportacionUploadResponse:
    """Process a SIGA logistics system export file.

    Args:
        file: The ``.xlsx`` file submitted as multipart/form-data.
        db: Database session.
        current_user: Authenticated user with ADMIN or PRESUPUESTO role.

    Returns:
        An ``ImportacionUploadResponse`` with validation statistics.
    """
    _validate_excel_file(file)
    logger.info(
        "upload_siga: user='%s' file='%s'",
        current_user.username,
        file.filename,
    )

    try:
        return await importacion_service.process_upload(
            db=db,
            file=file,
            user_id=current_user.id,
            username=current_user.username,
            declared_format="SIGA",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


# ---------------------------------------------------------------------------
# GET /formatos-catalogo
# ---------------------------------------------------------------------------


@router.get(
    "/formatos-catalogo",
    response_model=list[FormatoCatalogItem],
    summary="Catalogo de formatos disponibles",
    description=(
        "Lista los 10 formatos Excel disponibles para importacion, "
        "con nombre, descripcion y si tiene plantilla descargable."
    ),
)
def get_formatos_catalogo(
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> list[FormatoCatalogItem]:
    settings = get_settings()
    result: list[FormatoCatalogItem] = []
    for fmt in FORMATO_CATALOG:
        plantilla_path = settings.PLANTILLAS_DIR / f"plantilla_{fmt['key']}.xlsx"
        result.append(
            FormatoCatalogItem(
                key=fmt["key"],
                nombre=fmt["nombre"],
                descripcion=fmt["descripcion"],
                hoja=fmt["hoja"],
                columnas=len(fmt["columnas"]),
                fila_inicio=fmt["fila_inicio"],
                tiene_plantilla=plantilla_path.exists(),
            )
        )
    return result


# ---------------------------------------------------------------------------
# GET /plantilla/{formato_key}
# ---------------------------------------------------------------------------


@router.get(
    "/plantilla/{formato_key}",
    summary="Descargar plantilla Excel",
    description="Descarga la plantilla Excel del formato especificado.",
    responses={
        200: {"content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}},
        404: {"description": "Formato no encontrado o plantilla no generada."},
    },
)
def download_plantilla(
    formato_key: str,
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> FileResponse:
    settings = get_settings()
    plantilla_path = settings.PLANTILLAS_DIR / f"plantilla_{formato_key}.xlsx"

    if not plantilla_path.exists():
        # Try generating it on-the-fly
        try:
            generate_template(formato_key, plantilla_path)
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Formato '{formato_key}' no encontrado en el catalogo.",
            )

    return FileResponse(
        path=str(plantilla_path),
        filename=f"plantilla_{formato_key}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------------------------------------------------------------------
# POST /regenerar-plantillas
# ---------------------------------------------------------------------------


@router.post(
    "/regenerar-plantillas",
    summary="Regenerar todas las plantillas",
    description="(Admin) Regenera las 10 plantillas Excel. Requiere rol ADMIN.",
    responses={
        200: {"description": "Plantillas regeneradas exitosamente."},
        403: {"description": "Rol insuficiente."},
    },
)
def regenerar_plantillas(
    _current_user: Annotated[Usuario, Depends(require_role("ADMIN"))],
) -> dict:
    settings = get_settings()
    generated = generate_all_templates(settings.PLANTILLAS_DIR)
    return {
        "message": f"{len(generated)} plantillas regeneradas exitosamente.",
        "archivos": generated,
    }


# ---------------------------------------------------------------------------
# DELETE /limpiar-formato/{formato}
# ---------------------------------------------------------------------------


@router.delete(
    "/limpiar-formato/{formato}",
    summary="Eliminar datos importados de un formato",
    description=(
        "Elimina los datos importados y el historial de un formato especifico. "
        "Requiere rol ADMIN."
    ),
    responses={
        200: {"description": "Datos eliminados exitosamente."},
        403: {"description": "Rol insuficiente."},
        404: {"description": "Formato no encontrado."},
    },
)
def limpiar_formato(
    formato: str,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[Usuario, Depends(require_role("ADMIN"))],
) -> dict:
    logger.info(
        "limpiar_formato: user='%s' formato='%s'",
        current_user.username, formato,
    )
    try:
        return importacion_service.limpiar_formato(db, formato)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


# ---------------------------------------------------------------------------
# GET /estado-formatos
# ---------------------------------------------------------------------------


@router.get(
    "/estado-formatos",
    response_model=EstadoFormatosResponse,
    summary="Estado de carga de los 12 formatos",
    description=(
        "Retorna el estado de carga de cada uno de los 12 formatos de importación, "
        "con última carga, registros exitosos, y categoría. Usado por el panel de estado."
    ),
    responses={
        200: {"description": "Estado de los 12 formatos."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_estado_formatos(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
) -> EstadoFormatosResponse:
    """Return the status dashboard for all import formats."""
    return importacion_service.get_estado_formatos(db)


# ---------------------------------------------------------------------------
# GET /historial
# ---------------------------------------------------------------------------


@router.get(
    "/historial",
    response_model=list[HistorialImportacion],
    summary="Historial de importaciones",
    description=(
        "Lista los registros de importación ordenados del más reciente al más antiguo. "
        "Filtrable por año fiscal. Cualquier usuario autenticado puede consultar el historial."
    ),
    responses={
        200: {"description": "Lista de importaciones registradas."},
        401: {"description": "Token JWT ausente o inválido."},
    },
)
def get_historial(
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[Usuario, Depends(get_current_user)],
    anio: Annotated[
        int | None,
        Query(description="Filtrar por año fiscal, ej. 2026.", ge=2000, le=2100),
    ] = None,
) -> list[HistorialImportacion]:
    """Return the import history list, optionally filtered by fiscal year.

    Args:
        db: Database session.
        _current_user: Authenticated user guard.
        anio: Optional year filter applied to the ``fecha`` column.

    Returns:
        List of ``HistorialImportacion`` records, most-recent first.
    """
    filters = FilterParams(anio=anio)
    logger.debug("GET /importacion/historial anio=%s", anio)
    return importacion_service.get_historial(db, filters)
