"""
PDF export helper wrapping reportlab.

Provides ``PdfExporter`` — a stateful builder that constructs a styled
INEI Dashboard PDF document in memory and returns its bytes for streaming
via FastAPI's ``StreamingResponse``.

Usage example::

    exporter = PdfExporter(title="Presupuesto 2026", filters={"Año": "2026"})
    exporter.add_header()
    exporter.add_kpi_section(kpis)
    exporter.add_table(headers, rows)
    file_bytes = exporter.build()

Design notes
------------
- Uses ``reportlab``'s ``SimpleDocTemplate`` with ``Platypus`` story elements.
- Page layout: A4 landscape for data-heavy tables; portrait for narrow tables.
- Each page includes a footer with page number and generation timestamp.
- Colour palette matches the frontend design tokens.
- Table rows alternate white / light-grey for readability.
- Long text cells are automatically wrapped via ``Paragraph`` inside the table.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Sequence

try:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm, mm
    from reportlab.platypus import (
        HRFlowable,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )
    _REPORTLAB_AVAILABLE = True
except ImportError:
    _REPORTLAB_AVAILABLE = False


# Design token colours as hex strings
_HEX_PRIMARY = "#3b82f6"
_HEX_DARK = "#1E3A5F"
_HEX_LIGHT_GREY = "#F3F4F6"
_HEX_MID_GREY = "#E5E7EB"
_HEX_TEXT = "#111827"
_HEX_WHITE = "#FFFFFF"
_HEX_SUCCESS = "#10b981"
_HEX_WARNING = "#f59e0b"
_HEX_DANGER = "#ef4444"


def _hex_to_rl_color(hex_color: str) -> Any:
    """Convert a CSS hex colour string to a reportlab ``Color`` object.

    Args:
        hex_color: Hex string such as ``"#3b82f6"`` (with or without ``#``).

    Returns:
        A ``reportlab.lib.colors.HexColor`` instance.
    """
    return colors.HexColor(hex_color)


class PdfExporter:
    """Stateful PDF document builder for INEI Dashboard exports.

    Creates a PDF with an institutional header, optional KPI summary
    section, and a styled data table.

    Args:
        title: Document title, e.g. ``"Presupuesto 2026"``.
        filters: Applied filter labels to display in the header.
        landscape_mode: If ``True``, uses A4 landscape; otherwise portrait.

    Raises:
        ImportError: If ``reportlab`` is not installed.
    """

    def __init__(
        self,
        title: str,
        filters: dict[str, str] | None = None,
        landscape_mode: bool = False,
    ) -> None:
        if not _REPORTLAB_AVAILABLE:
            raise ImportError(
                "reportlab is required for PDF export. "
                "Install it with: pip install reportlab"
            )

        self._title = title
        self._filters = filters or {}
        self._landscape = landscape_mode

        self._buffer = io.BytesIO()
        page_size = landscape(A4) if landscape_mode else A4

        self._doc = SimpleDocTemplate(
            self._buffer,
            pagesize=page_size,
            rightMargin=1.5 * cm,
            leftMargin=1.5 * cm,
            topMargin=2 * cm,
            bottomMargin=2 * cm,
            title=f"INEI — {title}",
            author="Sistema Dashboard INEI",
        )

        self._story: list[Any] = []
        self._styles = getSampleStyleSheet()
        self._gen_ts = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
        self._custom_styles = self._build_styles()

    # -----------------------------------------------------------------------
    # Style factory
    # -----------------------------------------------------------------------

    def _build_styles(self) -> dict[str, ParagraphStyle]:
        """Register custom paragraph styles for use throughout the document.

        Returns:
            Dict mapping style name to ``ParagraphStyle`` instance.
        """
        styles: dict[str, ParagraphStyle] = {}

        styles["title"] = ParagraphStyle(
            "inei_title",
            fontName="Helvetica-Bold",
            fontSize=18,
            textColor=_hex_to_rl_color(_HEX_WHITE),
            alignment=TA_CENTER,
            spaceAfter=0,
        )

        styles["subtitle"] = ParagraphStyle(
            "inei_subtitle",
            fontName="Helvetica",
            fontSize=9,
            textColor=_hex_to_rl_color(_HEX_WHITE),
            alignment=TA_CENTER,
            spaceAfter=0,
        )

        styles["filter_key"] = ParagraphStyle(
            "filter_key",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=_hex_to_rl_color(_HEX_DARK),
            alignment=TA_RIGHT,
        )

        styles["filter_value"] = ParagraphStyle(
            "filter_value",
            fontName="Helvetica",
            fontSize=8,
            textColor=_hex_to_rl_color(_HEX_TEXT),
            alignment=TA_LEFT,
        )

        styles["kpi_label"] = ParagraphStyle(
            "kpi_label",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=_hex_to_rl_color(_HEX_DARK),
            alignment=TA_CENTER,
        )

        styles["kpi_value"] = ParagraphStyle(
            "kpi_value",
            fontName="Helvetica-Bold",
            fontSize=13,
            textColor=_hex_to_rl_color(_HEX_PRIMARY),
            alignment=TA_CENTER,
        )

        styles["section_heading"] = ParagraphStyle(
            "section_heading",
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=_hex_to_rl_color(_HEX_DARK),
            spaceBefore=8,
            spaceAfter=4,
        )

        styles["table_header"] = ParagraphStyle(
            "table_header",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=_hex_to_rl_color(_HEX_WHITE),
            alignment=TA_CENTER,
        )

        styles["table_cell"] = ParagraphStyle(
            "table_cell",
            fontName="Helvetica",
            fontSize=8,
            textColor=_hex_to_rl_color(_HEX_TEXT),
            alignment=TA_LEFT,
        )

        styles["table_cell_right"] = ParagraphStyle(
            "table_cell_right",
            fontName="Helvetica",
            fontSize=8,
            textColor=_hex_to_rl_color(_HEX_TEXT),
            alignment=TA_RIGHT,
        )

        return styles

    # -----------------------------------------------------------------------
    # Page template (footer)
    # -----------------------------------------------------------------------

    def _on_page(self, canvas: Any, doc: Any) -> None:
        """Render the page footer with page number and generation timestamp.

        This is attached as the ``onPage`` callback in ``SimpleDocTemplate.build``.

        Args:
            canvas: The current reportlab canvas.
            doc: The document template instance.
        """
        canvas.saveState()
        footer_text = (
            f"Sistema Dashboard INEI  |  Generado: {self._gen_ts}  |  "
            f"Página {doc.page}"
        )
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(_hex_to_rl_color(_HEX_MID_GREY))
        page_width = self._doc.pagesize[0]
        canvas.drawCentredString(page_width / 2, 1.2 * cm, footer_text)
        canvas.restoreState()

    # -----------------------------------------------------------------------
    # Public builder methods
    # -----------------------------------------------------------------------

    def add_header(self) -> "PdfExporter":
        """Add the INEI institutional header block to the document story.

        Creates a blue header table containing the document title and
        generation timestamp, followed by a filter summary table if any
        filters were provided.

        Returns:
            ``self`` for method chaining.
        """
        story = self._story
        primary = _hex_to_rl_color(_HEX_PRIMARY)
        dark = _hex_to_rl_color(_HEX_DARK)
        white = _hex_to_rl_color(_HEX_WHITE)

        # Main header band (coloured table with one merged cell)
        header_data = [
            [Paragraph(f"INEI — {self._title}", self._custom_styles["title"])],
            [Paragraph(f"Generado: {self._gen_ts}", self._custom_styles["subtitle"])],
        ]
        page_width = self._doc.width
        header_table = Table(header_data, colWidths=[page_width])
        header_table.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (0, 0), primary),
                ("BACKGROUND", (0, 1), (0, 1), dark),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ])
        )
        story.append(header_table)
        story.append(Spacer(1, 4 * mm))

        # Filter summary table
        if self._filters:
            filter_data = [
                [
                    Paragraph(f"{k}:", self._custom_styles["filter_key"]),
                    Paragraph(str(v), self._custom_styles["filter_value"]),
                ]
                for k, v in self._filters.items()
            ]
            filter_table = Table(
                filter_data,
                colWidths=[3 * cm, page_width - 3 * cm],
            )
            filter_table.setStyle(
                TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), _hex_to_rl_color(_HEX_LIGHT_GREY)),
                    ("GRID", (0, 0), (-1, -1), 0.25, _hex_to_rl_color(_HEX_MID_GREY)),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ])
            )
            story.append(filter_table)
            story.append(Spacer(1, 6 * mm))

        return self

    def add_kpi_section(self, kpis: dict[str, Any]) -> "PdfExporter":
        """Add a KPI summary section with labelled value cards.

        Renders KPI pairs as a single-row table with alternating label/value
        cells, matching the dashboard KPI card layout.

        Args:
            kpis: Ordered ``{label: value}`` dict.
                  Values may be ``float`` (rendered with 2 d.p.) or ``str``.

        Returns:
            ``self`` for method chaining.
        """
        story = self._story
        story.append(
            Paragraph("Indicadores Clave", self._custom_styles["section_heading"])
        )
        story.append(HRFlowable(width="100%", thickness=1, color=_hex_to_rl_color(_HEX_PRIMARY)))
        story.append(Spacer(1, 3 * mm))

        labels_row: list[Any] = []
        values_row: list[Any] = []

        for label, value in kpis.items():
            labels_row.append(Paragraph(label, self._custom_styles["kpi_label"]))
            if isinstance(value, float):
                val_str = f"{value:,.2f}"
            else:
                val_str = str(value)
            values_row.append(Paragraph(val_str, self._custom_styles["kpi_value"]))

        n = len(kpis)
        if n == 0:
            return self

        col_width = self._doc.width / n
        kpi_table = Table(
            [labels_row, values_row],
            colWidths=[col_width] * n,
        )
        kpi_table.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), _hex_to_rl_color("#EFF6FF")),
                ("BACKGROUND", (0, 1), (-1, 1), _hex_to_rl_color("#DBEAFE")),
                ("BOX", (0, 0), (-1, -1), 0.5, _hex_to_rl_color(_HEX_PRIMARY)),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, _hex_to_rl_color(_HEX_MID_GREY)),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ])
        )

        story.append(kpi_table)
        story.append(Spacer(1, 6 * mm))
        return self

    def add_table(
        self,
        headers: Sequence[str],
        rows: Sequence[Sequence[Any]],
        col_widths: Sequence[float] | None = None,
        numeric_cols: set[int] | None = None,
        section_title: str = "Detalle",
    ) -> "PdfExporter":
        """Add a styled data table to the document.

        Args:
            headers: Column header strings.
            rows: Data rows — inner sequences must match the header length.
            col_widths: Optional explicit column widths in cm.  If ``None``,
                        columns are distributed evenly across the page width.
            numeric_cols: Zero-based column indices with numeric content
                          (right-aligned).  Auto-detected from first row if
                          ``None``.
            section_title: Heading displayed above the table.

        Returns:
            ``self`` for method chaining.
        """
        story = self._story
        page_width = self._doc.width
        n_cols = len(headers)

        story.append(
            Paragraph(section_title, self._custom_styles["section_heading"])
        )
        story.append(HRFlowable(width="100%", thickness=1, color=_hex_to_rl_color(_HEX_PRIMARY)))
        story.append(Spacer(1, 3 * mm))

        # Resolve column widths
        if col_widths is not None:
            computed_widths = [w * cm for w in col_widths]
        else:
            computed_widths = [page_width / n_cols] * n_cols

        # Auto-detect numeric columns
        if numeric_cols is None:
            numeric_cols = set()
            if rows:
                for ci, val in enumerate(rows[0]):
                    if isinstance(val, (int, float)):
                        numeric_cols.add(ci)

        # Build table data (headers + data rows as Paragraph objects)
        header_row = [
            Paragraph(str(h), self._custom_styles["table_header"])
            for h in headers
        ]
        table_data: list[list[Any]] = [header_row]

        for data_row in rows:
            pdf_row: list[Any] = []
            for ci, cell_val in enumerate(data_row):
                style_name = "table_cell_right" if ci in numeric_cols else "table_cell"
                cell_style = self._custom_styles[style_name]
                if isinstance(cell_val, float):
                    text = f"{cell_val:,.2f}"
                else:
                    text = str(cell_val) if cell_val is not None else ""
                pdf_row.append(Paragraph(text, cell_style))
            table_data.append(pdf_row)

        rl_table = Table(table_data, colWidths=computed_widths, repeatRows=1)

        # Build alternating row shading commands
        style_cmds: list[tuple[Any, ...]] = [
            ("BACKGROUND", (0, 0), (-1, 0), _hex_to_rl_color(_HEX_DARK)),
            ("TEXTCOLOR", (0, 0), (-1, 0), _hex_to_rl_color(_HEX_WHITE)),
            ("GRID", (0, 0), (-1, -1), 0.25, _hex_to_rl_color(_HEX_MID_GREY)),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]

        for ri in range(1, len(table_data)):
            if ri % 2 == 0:
                style_cmds.append(
                    ("BACKGROUND", (0, ri), (-1, ri), _hex_to_rl_color(_HEX_LIGHT_GREY))
                )
            else:
                style_cmds.append(
                    ("BACKGROUND", (0, ri), (-1, ri), _hex_to_rl_color(_HEX_WHITE))
                )

        rl_table.setStyle(TableStyle(style_cmds))
        story.append(rl_table)
        story.append(Spacer(1, 4 * mm))
        return self

    def build(self) -> bytes:
        """Build the PDF document and return its bytes.

        After calling ``build`` the exporter instance should not be reused.

        Returns:
            Raw bytes of the ``.pdf`` file ready for streaming.
        """
        self._doc.build(
            self._story,
            onFirstPage=self._on_page,
            onLaterPages=self._on_page,
        )
        self._buffer.seek(0)
        return self._buffer.read()
