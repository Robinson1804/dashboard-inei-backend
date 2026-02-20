"""
Excel export helper wrapping xlsxwriter.

Provides ``ExcelExporter`` — a stateful builder that constructs a styled
INEI Dashboard Excel workbook in memory and returns its bytes for streaming
via FastAPI's ``StreamingResponse``.

Usage example::

    exporter = ExcelExporter(title="Presupuesto 2026", filters={"Año": "2026"})
    exporter.add_header()
    exporter.add_kpi_row(kpis)
    exporter.add_data_table(headers, rows)
    file_bytes = exporter.finalize()

Design notes
------------
- Uses ``xlsxwriter`` in in-memory mode (``BytesIO``).
- Column widths are auto-sized based on the maximum content length in each
  column (capped at 60 characters to avoid excessively wide columns).
- All monetary values use the Peruvian soles format ``#,##0.00``.
- Alternating row shading uses light-grey every other data row.
- The INEI institutional header spans all columns and uses the primary
  dashboard colour ``#3b82f6``.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Sequence


try:
    import xlsxwriter
    from xlsxwriter.workbook import Workbook
    from xlsxwriter.worksheet import Worksheet
    _XLSXWRITER_AVAILABLE = True
except ImportError:
    _XLSXWRITER_AVAILABLE = False


# Design token colours (from CLAUDE.md)
_COLOR_PRIMARY = "#3b82f6"   # INEI blue
_COLOR_SUCCESS = "#10b981"   # green
_COLOR_WARNING = "#f59e0b"   # amber
_COLOR_DANGER = "#ef4444"    # red
_COLOR_WHITE = "#FFFFFF"
_COLOR_LIGHT_GREY = "#F3F4F6"
_COLOR_HEADER_TEXT = "#FFFFFF"
_COLOR_SUBHEADER_BG = "#1E3A5F"

_MAX_COL_WIDTH = 60
_MIN_COL_WIDTH = 8


class ExcelExporter:
    """Stateful Excel workbook builder for INEI Dashboard exports.

    Creates a single worksheet with an institutional header, optional KPI
    summary row, and a styled data table.

    Args:
        title: Workbook and sheet title, e.g. ``"Presupuesto 2026"``.
        filters: Dict of applied filter labels to display in the header,
                 e.g. ``{"Año": "2026", "UE": "INEI-LIMA"}``.
        sheet_name: Name of the worksheet tab (default: ``"Datos"``).

    Raises:
        ImportError: If ``xlsxwriter`` is not installed.
    """

    def __init__(
        self,
        title: str,
        filters: dict[str, str] | None = None,
        sheet_name: str = "Datos",
    ) -> None:
        if not _XLSXWRITER_AVAILABLE:
            raise ImportError(
                "xlsxwriter is required for Excel export. "
                "Install it with: pip install xlsxwriter"
            )

        self._title = title
        self._filters = filters or {}
        self._sheet_name = sheet_name

        self._buffer = io.BytesIO()
        self._workbook: Workbook = xlsxwriter.Workbook(self._buffer, {"in_memory": True})
        self._worksheet: Worksheet = self._workbook.add_worksheet(sheet_name)

        # Track current write row
        self._current_row: int = 0
        # Track number of columns (set when add_data_table is called)
        self._num_cols: int = 1

        # Pre-build common formats
        self._formats: dict[str, Any] = self._build_formats()

    # -----------------------------------------------------------------------
    # Format factory
    # -----------------------------------------------------------------------

    def _build_formats(self) -> dict[str, Any]:
        """Create and register all cell formats used by the workbook.

        Returns:
            Dict mapping format name to ``xlsxwriter`` format object.
        """
        wb = self._workbook
        formats: dict[str, Any] = {}

        # Institutional header — large bold white text on INEI blue
        formats["header_main"] = wb.add_format({
            "bold": True,
            "font_size": 16,
            "font_color": _COLOR_WHITE,
            "bg_color": _COLOR_PRIMARY,
            "align": "center",
            "valign": "vcenter",
            "border": 0,
        })

        # Sub-header row — subtitle / generation timestamp
        formats["header_sub"] = wb.add_format({
            "bold": False,
            "font_size": 10,
            "font_color": _COLOR_WHITE,
            "bg_color": _COLOR_SUBHEADER_BG,
            "align": "center",
            "valign": "vcenter",
            "border": 0,
        })

        # Filter label cell (key)
        formats["filter_key"] = wb.add_format({
            "bold": True,
            "font_size": 9,
            "font_color": "#374151",
            "bg_color": "#E5E7EB",
            "align": "right",
            "valign": "vcenter",
            "right": 1,
            "right_color": "#D1D5DB",
        })

        # Filter value cell
        formats["filter_value"] = wb.add_format({
            "bold": False,
            "font_size": 9,
            "font_color": "#111827",
            "bg_color": "#F9FAFB",
            "align": "left",
            "valign": "vcenter",
        })

        # KPI label
        formats["kpi_label"] = wb.add_format({
            "bold": True,
            "font_size": 10,
            "font_color": "#374151",
            "bg_color": "#EFF6FF",
            "align": "center",
            "valign": "vcenter",
            "top": 1,
            "bottom": 1,
            "left": 1,
            "right": 1,
            "border_color": "#BFDBFE",
        })

        # KPI value
        formats["kpi_value"] = wb.add_format({
            "bold": True,
            "font_size": 12,
            "font_color": _COLOR_PRIMARY,
            "bg_color": "#EFF6FF",
            "align": "center",
            "valign": "vcenter",
            "num_format": "#,##0.00",
            "top": 1,
            "bottom": 1,
            "left": 1,
            "right": 1,
            "border_color": "#BFDBFE",
        })

        # Table column headers — bold white on dark blue
        formats["col_header"] = wb.add_format({
            "bold": True,
            "font_size": 10,
            "font_color": _COLOR_WHITE,
            "bg_color": _COLOR_SUBHEADER_BG,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#CBD5E1",
            "text_wrap": True,
        })

        # Data row — plain
        formats["data_plain"] = wb.add_format({
            "font_size": 9,
            "font_color": "#111827",
            "bg_color": _COLOR_WHITE,
            "align": "left",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#E5E7EB",
        })

        # Data row — alternate shading
        formats["data_alt"] = wb.add_format({
            "font_size": 9,
            "font_color": "#111827",
            "bg_color": _COLOR_LIGHT_GREY,
            "align": "left",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#E5E7EB",
        })

        # Numeric data row — right-aligned, money format
        formats["data_number"] = wb.add_format({
            "font_size": 9,
            "font_color": "#111827",
            "bg_color": _COLOR_WHITE,
            "align": "right",
            "valign": "vcenter",
            "num_format": "#,##0.00",
            "border": 1,
            "border_color": "#E5E7EB",
        })

        formats["data_number_alt"] = wb.add_format({
            "font_size": 9,
            "font_color": "#111827",
            "bg_color": _COLOR_LIGHT_GREY,
            "align": "right",
            "valign": "vcenter",
            "num_format": "#,##0.00",
            "border": 1,
            "border_color": "#E5E7EB",
        })

        # Percentage format
        formats["data_pct"] = wb.add_format({
            "font_size": 9,
            "font_color": "#111827",
            "bg_color": _COLOR_WHITE,
            "align": "right",
            "valign": "vcenter",
            "num_format": "0.00%",
            "border": 1,
            "border_color": "#E5E7EB",
        })

        return formats

    # -----------------------------------------------------------------------
    # Public builder methods
    # -----------------------------------------------------------------------

    def add_header(self) -> "ExcelExporter":
        """Write the INEI institutional header block to the worksheet.

        Adds:
        1. A merged title row spanning all columns (height 30px).
        2. A generation-date subtitle row.
        3. One row per filter key/value pair (if filters were provided).

        Returns:
            ``self`` for method chaining.
        """
        ws = self._worksheet
        fmt_main = self._formats["header_main"]
        fmt_sub = self._formats["header_sub"]
        fmt_key = self._formats["filter_key"]
        fmt_val = self._formats["filter_value"]

        # Reserve enough columns for the header — will be adjusted after
        # add_data_table is called, but we need a starting estimate.
        num_cols = max(self._num_cols, 6)

        # Row 0 — main title
        ws.set_row(self._current_row, 32)
        ws.merge_range(
            self._current_row, 0,
            self._current_row, num_cols - 1,
            f"INEI — {self._title}",
            fmt_main,
        )
        self._current_row += 1

        # Row 1 — generation timestamp
        gen_ts = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
        ws.set_row(self._current_row, 18)
        ws.merge_range(
            self._current_row, 0,
            self._current_row, num_cols - 1,
            f"Generado: {gen_ts}",
            fmt_sub,
        )
        self._current_row += 1

        # Filter rows
        for key, value in self._filters.items():
            ws.set_row(self._current_row, 16)
            ws.write(self._current_row, 0, key, fmt_key)
            ws.merge_range(
                self._current_row, 1,
                self._current_row, num_cols - 1,
                value,
                fmt_val,
            )
            self._current_row += 1

        # Blank separator row
        self._current_row += 1

        return self

    def add_kpi_row(self, kpis: dict[str, Any]) -> "ExcelExporter":
        """Write a single KPI summary row with labelled value cells.

        Each key/value pair in ``kpis`` is written as a label cell above
        a value cell, grouped horizontally.  Monetary values (``float``) use
        the money number format; strings are written as-is.

        Args:
            kpis: Ordered dict of ``{label: value}`` pairs to display.
                  Example: ``{"PIM Total": 245_000_000.0, "Ejecución": "71.56%"}``.

        Returns:
            ``self`` for method chaining.
        """
        ws = self._worksheet
        fmt_label = self._formats["kpi_label"]
        fmt_value = self._formats["kpi_value"]

        col = 0
        for label, value in kpis.items():
            ws.set_row(self._current_row, 16)
            ws.write(self._current_row, col, label, fmt_label)
            ws.set_row(self._current_row + 1, 22)
            ws.write(self._current_row + 1, col, value, fmt_value)
            col += 1

        self._current_row += 3  # label row + value row + blank separator
        return self

    def add_data_table(
        self,
        headers: Sequence[str],
        rows: Sequence[Sequence[Any]],
        numeric_cols: set[int] | None = None,
    ) -> "ExcelExporter":
        """Write a styled data table with alternating row shading.

        Args:
            headers: Column header strings.
            rows: Data rows — each inner sequence must match the length of
                  ``headers``.
            numeric_cols: Zero-based column indices that contain numeric
                          values and should use the money/right-aligned format.
                          If ``None``, columns are auto-detected as numeric when
                          the first data row contains ``int`` or ``float`` values.

        Returns:
            ``self`` for method chaining.
        """
        ws = self._worksheet
        num_cols = len(headers)
        self._num_cols = num_cols

        fmt_col_hdr = self._formats["col_header"]

        # --- Auto-detect numeric columns from first data row ---
        if numeric_cols is None:
            numeric_cols = set()
            if rows:
                for ci, val in enumerate(rows[0]):
                    if isinstance(val, (int, float)):
                        numeric_cols.add(ci)

        # Track max content width per column for auto-sizing
        col_widths: list[int] = [len(str(h)) for h in headers]

        # Write column headers
        ws.set_row(self._current_row, 20)
        for ci, hdr in enumerate(headers):
            ws.write(self._current_row, ci, hdr, fmt_col_hdr)
        self._current_row += 1

        # Write data rows
        for ri, data_row in enumerate(rows):
            is_alt = ri % 2 == 1
            ws.set_row(self._current_row, 15)

            for ci, cell_val in enumerate(data_row):
                is_numeric = ci in numeric_cols
                if is_numeric:
                    fmt = self._formats["data_number_alt"] if is_alt else self._formats["data_number"]
                else:
                    fmt = self._formats["data_alt"] if is_alt else self._formats["data_plain"]

                ws.write(self._current_row, ci, cell_val, fmt)

                # Update column width estimate
                cell_str = str(cell_val) if cell_val is not None else ""
                col_widths[ci] = min(
                    _MAX_COL_WIDTH,
                    max(col_widths[ci], len(cell_str)),
                )

            self._current_row += 1

        # Apply auto-column widths
        for ci, width in enumerate(col_widths):
            ws.set_column(ci, ci, max(width + 2, _MIN_COL_WIDTH))

        return self

    def finalize(self) -> bytes:
        """Close the workbook and return its bytes content.

        After calling ``finalize`` the exporter instance should not be reused.

        Returns:
            Raw bytes of the ``.xlsx`` file ready for streaming.
        """
        self._workbook.close()
        self._buffer.seek(0)
        return self._buffer.read()
