"""
Export service layer.

Coordinates data retrieval and format conversion for all export endpoints.
Reuses the existing service functions from the domain service modules to
avoid duplicating query logic, then hands the resulting data to the
``ExcelExporter`` and ``PdfExporter`` builder classes.

Supported modules
-----------------
- ``"presupuesto"``             — Budget execution table.
- ``"adquisiciones"``           — Acquisition process table (placeholder).
- ``"contratos_menores"``       — Minor contracts table (placeholder).
- ``"actividades_operativas"``  — AO execution table.

Design notes
------------
- Both ``export_excel`` and ``export_pdf`` call the same module-dispatch
  helper so that column definitions and data fetching live in a single place.
- ``PaginationParams(page=1, page_size=5000)`` is used to retrieve a large
  but bounded result set — this avoids unbounded queries in production.
- Monetary columns are identified by index so that the exporter helpers can
  apply the correct number format.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.orm import Session

from app.exporters.excel_exporter import ExcelExporter
from app.exporters.pdf_exporter import PdfExporter
from app.schemas.common import FilterParams
from app.services import ao_service, presupuesto_service

logger = logging.getLogger(__name__)


class _ExportPagination:
    """Lightweight pagination for exports — bypasses PaginationParams validation cap."""
    page: int = 1
    page_size: int = 5_000


# Large but bounded page used for exports (5,000 rows maximum per export)
_EXPORT_PAGINATION = _ExportPagination()


# ---------------------------------------------------------------------------
# Module dispatch helper
# ---------------------------------------------------------------------------


def _get_export_data(
    db: Session,
    modulo: str,
    filters: FilterParams,
) -> tuple[str, list[str], list[list[Any]], dict[str, Any], set[int]]:
    """Retrieve and normalise data for the requested export module.

    Args:
        db: Active SQLAlchemy session.
        modulo: One of the four supported module keys.
        filters: Year, UE, and other filter constraints.

    Returns:
        A 5-tuple of:
        - ``report_title``: Human-readable title for the report.
        - ``headers``: List of column header strings.
        - ``rows``: List of data rows (each a list of cell values).
        - ``kpis``: Ordered dict of KPI label → value for the summary section.
        - ``numeric_cols``: Set of zero-based column indices with numeric data.

    Raises:
        ValueError: If ``modulo`` is not one of the four supported values.
    """
    modulo_lower = modulo.lower()

    if modulo_lower == "presupuesto":
        return _data_presupuesto(db, filters)
    if modulo_lower == "actividades_operativas":
        return _data_ao(db, filters)
    if modulo_lower in ("adquisiciones", "contratos_menores"):
        return _data_placeholder(modulo_lower)
    raise ValueError(
        f"Módulo de exportación no reconocido: '{modulo}'. "
        f"Valores válidos: presupuesto, adquisiciones, contratos_menores, actividades_operativas."
    )


def _data_presupuesto(
    db: Session,
    filters: FilterParams,
) -> tuple[str, list[str], list[list[Any]], dict[str, Any], set[int]]:
    """Fetch presupuesto data for export.

    Args:
        db: Active SQLAlchemy session.
        filters: Budget filter constraints.

    Returns:
        Export tuple as described in ``_get_export_data``.
    """
    # KPIs
    kpi_obj = presupuesto_service.get_kpis(db, filters)
    kpis: dict[str, Any] = {
        "UEs": kpi_obj.total_ues,
        "Metas": kpi_obj.total_metas,
        "PIM Total (S/)": kpi_obj.pim_total,
        "Certificado (S/)": kpi_obj.certificado_total,
        "Devengado (S/)": kpi_obj.devengado_total,
        "Ejecución (%)": f"{kpi_obj.ejecucion_porcentaje:.2f}%",
    }

    # Table rows
    tabla = presupuesto_service.get_tabla(db, filters, _EXPORT_PAGINATION)
    headers = [
        "ID",
        "Unidad Ejecutora",
        "Meta",
        "Clasificador",
        "Descripción",
        "PIM (S/)",
        "Certificado (S/)",
        "Devengado (S/)",
        "Saldo (S/)",
        "Ejecución (%)",
    ]
    numeric_cols = {5, 6, 7, 8, 9}

    rows: list[list[Any]] = [
        [
            r.id,
            r.ue,
            r.meta,
            r.clasificador,
            r.descripcion,
            r.pim,
            r.certificado,
            r.devengado,
            r.saldo,
            r.ejecucion,
        ]
        for r in tabla.rows
    ]

    title = "Ejecución Presupuestal"
    return title, headers, rows, kpis, numeric_cols


def _data_ao(
    db: Session,
    filters: FilterParams,
) -> tuple[str, list[str], list[list[Any]], dict[str, Any], set[int]]:
    """Fetch Actividades Operativas data for export.

    Args:
        db: Active SQLAlchemy session.
        filters: AO filter constraints.

    Returns:
        Export tuple as described in ``_get_export_data``.
    """
    # KPIs
    kpi_obj = ao_service.get_kpis(db, filters)
    kpis: dict[str, Any] = {
        "Total AOs": kpi_obj.total_aos,
        "VERDE": kpi_obj.verdes,
        "AMARILLO": kpi_obj.amarillos,
        "ROJO": kpi_obj.rojos,
        "% Verde": f"{kpi_obj.porcentaje_verde:.1f}%",
    }

    # Table rows
    tabla = ao_service.get_tabla(db, filters, _EXPORT_PAGINATION)
    headers = [
        "ID",
        "Código CEPLAN",
        "Nombre de la Actividad",
        "Unidad Ejecutora",
        "Programado (S/)",
        "Ejecutado (S/)",
        "Ejecución (%)",
        "Semáforo",
    ]
    numeric_cols = {4, 5, 6}

    rows: list[list[Any]] = [
        [
            r.id,
            r.codigo_ceplan,
            r.nombre,
            r.ue_sigla,
            r.programado_total,
            r.ejecutado_total,
            r.ejecucion_porcentaje,
            r.semaforo,
        ]
        for r in tabla.rows
    ]

    title = "Actividades Operativas"
    return title, headers, rows, kpis, numeric_cols


def _data_placeholder(
    modulo: str,
) -> tuple[str, list[str], list[list[Any]], dict[str, Any], set[int]]:
    """Return placeholder data for modules not yet fully implemented.

    Args:
        modulo: Module key (e.g. ``"adquisiciones"``).

    Returns:
        Export tuple with empty rows and a note in the KPI section.
    """
    title_map = {
        "adquisiciones": "Adquisiciones (>8 UIT)",
        "contratos_menores": "Contratos Menores (≤8 UIT)",
    }
    title = title_map.get(modulo, modulo.replace("_", " ").title())
    headers = ["Módulo", "Estado"]
    rows: list[list[Any]] = [[title, "Exportación completa disponible en próxima versión."]]
    kpis: dict[str, Any] = {"Módulo": title, "Estado": "En desarrollo"}
    return title, headers, rows, kpis, set()


# ---------------------------------------------------------------------------
# Public export functions
# ---------------------------------------------------------------------------


def export_excel(db: Session, modulo: str, filters: FilterParams) -> bytes:
    """Generate an Excel (.xlsx) export for the requested dashboard module.

    Retrieves data via the module's service layer and passes it to
    ``ExcelExporter`` to produce a styled workbook.

    Args:
        db: Active SQLAlchemy session.
        modulo: Export module key (see ``_get_export_data`` for valid values).
        filters: Year, UE, and other filter constraints.

    Returns:
        Raw bytes of the ``.xlsx`` file.

    Raises:
        ValueError: If ``modulo`` is not recognised.
        ImportError: If ``xlsxwriter`` is not installed.
    """
    title, headers, rows, kpis, numeric_cols = _get_export_data(db, modulo, filters)

    # Build human-readable filter description for the header
    filter_labels: dict[str, str] = {}
    if filters.anio is not None:
        filter_labels["Año fiscal"] = str(filters.anio)
    if filters.ue_id is not None:
        filter_labels["Unidad Ejecutora ID"] = str(filters.ue_id)
    if filters.fuente_financiamiento is not None:
        filter_labels["Fuente financiamiento"] = filters.fuente_financiamiento

    exporter = ExcelExporter(
        title=f"{title} {filters.anio or ''}".strip(),
        filters=filter_labels,
    )
    exporter.add_header()
    exporter.add_kpi_row(kpis)
    exporter.add_data_table(headers, rows, numeric_cols=numeric_cols)
    file_bytes = exporter.finalize()

    logger.info(
        "export_excel: modulo=%s rows=%d bytes=%d",
        modulo, len(rows), len(file_bytes),
    )
    return file_bytes


def export_pdf(db: Session, modulo: str, filters: FilterParams) -> bytes:
    """Generate a PDF export for the requested dashboard module.

    Retrieves data via the module's service layer and passes it to
    ``PdfExporter`` to produce a styled document.

    Args:
        db: Active SQLAlchemy session.
        modulo: Export module key (see ``_get_export_data`` for valid values).
        filters: Year, UE, and other filter constraints.

    Returns:
        Raw bytes of the ``.pdf`` file.

    Raises:
        ValueError: If ``modulo`` is not recognised.
        ImportError: If ``reportlab`` is not installed.
    """
    title, headers, rows, kpis, numeric_cols = _get_export_data(db, modulo, filters)

    filter_labels: dict[str, str] = {}
    if filters.anio is not None:
        filter_labels["Año fiscal"] = str(filters.anio)
    if filters.ue_id is not None:
        filter_labels["Unidad Ejecutora ID"] = str(filters.ue_id)
    if filters.fuente_financiamiento is not None:
        filter_labels["Fuente financiamiento"] = filters.fuente_financiamiento

    # Use landscape for wide tables (>6 columns)
    use_landscape = len(headers) > 6

    exporter = PdfExporter(
        title=f"{title} {filters.anio or ''}".strip(),
        filters=filter_labels,
        landscape_mode=use_landscape,
    )
    exporter.add_header()
    exporter.add_kpi_section(kpis)
    exporter.add_table(
        headers,
        rows,
        numeric_cols=numeric_cols,
        section_title=title,
    )
    file_bytes = exporter.build()

    logger.info(
        "export_pdf: modulo=%s rows=%d bytes=%d",
        modulo, len(rows), len(file_bytes),
    )
    return file_bytes
