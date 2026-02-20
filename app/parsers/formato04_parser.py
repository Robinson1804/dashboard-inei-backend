"""Parser for Formato 04 — Budget Modifications (Habilitaciones).

Sheet layout (rows are 1-based as in Excel):
    Rows 1–6:  Title block and institutional context
    Row 7:     Column headers
    Row 8+:    Data rows  (F8 in Excel = index 7 in 0-based)

Expected 6 columns:
    Clasificador | Descripcion | Asignado | Habilitadora | Habilitada | PIM Resultante

Business rule:
    PIM Resultante = Asignado + Habilitadora - Habilitada

Each data row maps to one ``ModificacionPresupuestal`` record.

The parser detects whether the modification type for each row is:
    - "HABILITACION" (Habilitadora > 0, i.e. receives funds)
    - "HABILITADA"   (Habilitada > 0, i.e. gives away funds)

Validation rules:
    - Clasificador code must match "X.X…" pattern.
    - PIM Resultante must be non-negative.
    - Computed PIM (Asignado + Habilitadora - Habilitada) must equal
      the declared PIM Resultante (±1 sol tolerance).
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParseResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CLASIFICADOR_RE = re.compile(r"^\d+(\.\d+){1,5}$")

_CONTEXT_POSITIONS: dict[str, tuple[int, int]] = {
    "ue_nombre":      (1, 2),
    "ue_codigo":      (1, 5),
    "nota_numero":    (2, 2),   # Nota de Modificación number
    "fecha":          (2, 5),
    "anio":           (3, 5),
}

_COL_ALIASES: dict[str, list[str]] = {
    "clasificador": [
        "clasificador", "código", "codigo", "cod. gasto", "cod gasto",
        "clasificador de gasto",
    ],
    "descripcion": [
        "descripcion", "descripción", "nombre", "denominacion",
        "descripcion del gasto",
    ],
    "asignado": [
        "asignado", "pia", "presupuesto asignado", "monto asignado",
        "inicial",
    ],
    "habilitadora": [
        "habilitadora", "habilitación", "habilitacion", "credito",
        "crédito", "+ habilitadora",
    ],
    "habilitada": [
        "habilitada", "débito", "debito", "- habilitada",
    ],
    "pim_resultante": [
        "pim resultante", "pim", "presupuesto institucional modificado",
        "resultante", "pim final",
    ],
}


def _match_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Find first DataFrame column matching any alias (case-insensitive)."""
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for alias in aliases:
        al = alias.lower().strip()
        if al in cols_lower:
            return cols_lower[al]
    for alias in aliases:
        al = alias.lower().strip()
        for col_lower, col_orig in cols_lower.items():
            if al in col_lower:
                return col_orig
    return None


class Formato04Parser(BaseParser):
    """Parse Formato 04 — budget modification records.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 7 (row 8 in Excel).
    """

    FORMAT_NAME = "FORMATO_04"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 7,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that required columns are present."""
        errors: list[str] = []
        required = {
            "clasificador": _COL_ALIASES["clasificador"],
            "habilitadora": _COL_ALIASES["habilitadora"],
            "habilitada":   _COL_ALIASES["habilitada"],
            "pim_resultante": _COL_ALIASES["pim_resultante"],
        }
        for field, aliases in required.items():
            if _match_column(df, aliases) is None:
                errors.append(
                    f"Formato04: columna '{field}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 04 parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Extract context metadata
        # ----------------------------------------------------------------
        raw_head = self._load_raw_rows(
            sheet_name=self.sheet_name,
            nrows=self.data_start_row,
        )
        context = self._extract_context(raw_head, _CONTEXT_POSITIONS)

        if not context.get("anio"):
            context["anio"] = self._scan_for_value(raw_head, "año")
        if not context.get("ue_codigo"):
            context["ue_codigo"] = self._scan_for_value(raw_head, "codigo ue")
        if not context.get("nota_numero"):
            context["nota_numero"] = self._scan_for_value(raw_head, "nota")
        if not context.get("fecha"):
            context["fecha"] = self._scan_for_value(raw_head, "fecha")

        self.result.metadata.update(context)

        try:
            anio = int(float(context.get("anio", "0") or "0"))
        except (ValueError, TypeError):
            anio = 0
            self.result.warnings.append(
                "Formato04: no se pudo determinar el año; se usará 0."
            )

        ue_codigo = context.get("ue_codigo", "")
        nota_numero = context.get("nota_numero", "")
        fecha_str = context.get("fecha", "")

        # Parse date if present
        fecha = _parse_date(fecha_str)

        # ----------------------------------------------------------------
        # 2. Detect header row
        # ----------------------------------------------------------------
        header_row_idx = self._detect_header_row(raw_head)

        # ----------------------------------------------------------------
        # 3. Load main DataFrame
        # ----------------------------------------------------------------
        df = self._load_sheet(
            sheet_name=self.sheet_name,
            header=header_row_idx,
            dtype=str,
        )
        if df.empty:
            self.result.errors.append("Formato04: la hoja está vacía.")
            return self.result

        df.columns = [self._clean_str(c) for c in df.columns]

        # ----------------------------------------------------------------
        # 4. Validate structure
        # ----------------------------------------------------------------
        struct_errors = self.validate_structure(df)
        self.result.errors.extend(struct_errors)
        if struct_errors:
            return self.result

        # ----------------------------------------------------------------
        # 5. Resolve columns
        # ----------------------------------------------------------------
        col_clas = _match_column(df, _COL_ALIASES["clasificador"])
        col_desc = _match_column(df, _COL_ALIASES["descripcion"])
        col_asig = _match_column(df, _COL_ALIASES["asignado"])
        col_hab_r = _match_column(df, _COL_ALIASES["habilitadora"])  # receiving
        col_hab_g = _match_column(df, _COL_ALIASES["habilitada"])    # giving
        col_pim = _match_column(df, _COL_ALIASES["pim_resultante"])

        # ----------------------------------------------------------------
        # 6. Iterate rows
        # ----------------------------------------------------------------
        rows_to_skip = max(0, self.data_start_row - header_row_idx - 1)
        skipped = 0
        valid_rows = 0

        for row_idx, row in df.iterrows():
            if int(row_idx) < rows_to_skip:
                continue
            if self._is_empty_row(row):
                continue
            header_kws = [
                "clasificador", "asignado", "habilitadora", "habilitada", "pim"
            ]
            if self._is_header_row(row, header_kws):
                continue

            # ----------------------------------------------------------
            # Classifier code (required)
            # ----------------------------------------------------------
            raw_clas = self._clean_str(row.get(col_clas, "")) if col_clas else ""
            clasificador = self._normalize_clasificador(raw_clas)

            if not clasificador or not _CLASIFICADOR_RE.match(clasificador):
                # Could be a subtotal/total row
                if clasificador:
                    self.result.warnings.append(
                        f"Fila {row_idx}: clasificador inválido "
                        f"('{clasificador}') — fila omitida."
                    )
                skipped += 1
                continue

            descripcion = (
                self._clean_str(row.get(col_desc, "")) if col_desc else ""
            )
            asignado = self._to_decimal(row.get(col_asig)) if col_asig else 0.0
            habilitadora = self._to_decimal(row.get(col_hab_r)) if col_hab_r else 0.0
            habilitada = self._to_decimal(row.get(col_hab_g)) if col_hab_g else 0.0
            pim_declarado = self._to_decimal(row.get(col_pim)) if col_pim else 0.0

            # ----------------------------------------------------------
            # Business rule validation
            # ----------------------------------------------------------
            pim_calculado = round(asignado + habilitadora - habilitada, 2)

            if col_pim and abs(pim_calculado - pim_declarado) > 1.0:
                self.result.warnings.append(
                    f"Fila {row_idx} clasificador '{clasificador}': "
                    f"PIM calculado ({pim_calculado:.2f}) ≠ declarado "
                    f"({pim_declarado:.2f}) — usando calculado."
                )
                pim_final = pim_calculado
            else:
                pim_final = pim_declarado if col_pim else pim_calculado

            if pim_final < 0:
                self.result.warnings.append(
                    f"Fila {row_idx} clasificador '{clasificador}': "
                    f"PIM resultante negativo ({pim_final:.2f}) — fila omitida."
                )
                skipped += 1
                continue

            # ----------------------------------------------------------
            # Determine modification type
            # ----------------------------------------------------------
            tipo = _determine_tipo(habilitadora, habilitada)

            # ----------------------------------------------------------
            # Emit record
            # ----------------------------------------------------------
            record: dict[str, Any] = {
                "_type": "modificacion_presupuestal",
                "anio": anio,
                "ue_codigo": ue_codigo,
                "clasificador_codigo": clasificador,
                "descripcion": descripcion,
                "tipo": tipo,
                "monto": round(max(habilitadora, habilitada), 2),
                "nota_modificacion": nota_numero,
                "fecha": fecha,
                "asignado": round(asignado, 2),
                "habilitadora": round(habilitadora, 2),
                "habilitada": round(habilitada, 2),
                "pim_resultante": round(pim_final, 2),
            }
            self.result.records.append(record)
            valid_rows += 1

        # ----------------------------------------------------------------
        # 7. Summary metadata
        # ----------------------------------------------------------------
        self.result.metadata.update(
            {
                "valid_rows": valid_rows,
                "skipped_rows": skipped,
                "anio": anio,
                "ue_codigo": ue_codigo,
                "nota_numero": nota_numero,
            }
        )

        logger.info(
            "Formato04Parser: rows=%d skipped=%d anio=%d ue=%s nota=%s",
            valid_rows,
            skipped,
            anio,
            ue_codigo,
            nota_numero,
        )
        return self.result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        """Find the 0-based row that contains column header keywords."""
        for r in range(min(10, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if any(
                kw in row_text
                for kw in ("habilitadora", "habilitada", "clasificador", "asignado")
            ):
                return r
        return max(0, self.data_start_row - 1)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _determine_tipo(habilitadora: float, habilitada: float) -> str:
    """Return the modification type based on which column has a value."""
    if habilitadora > 0 and habilitada == 0:
        return "HABILITACION"
    if habilitada > 0 and habilitadora == 0:
        return "HABILITADA"
    if habilitadora > 0 and habilitada > 0:
        # Both columns filled — net effect determines type
        return "HABILITACION" if habilitadora >= habilitada else "HABILITADA"
    return "HABILITACION"  # default


def _parse_date(date_str: str) -> str | None:
    """Attempt to normalise a date string to ISO format (YYYY-MM-DD).

    Returns None if parsing fails.
    """
    if not date_str:
        return None
    # Try common Peruvian date formats
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            from datetime import datetime

            return datetime.strptime(date_str.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return date_str  # Return as-is if unparseable
