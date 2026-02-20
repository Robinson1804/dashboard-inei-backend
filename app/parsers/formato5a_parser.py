"""Parser for Formato 5.A — AO Programming (Programado only).

Sheet layout (rows are 1-based as in Excel):
    Rows 1–8:  Title block and institutional context
    Row 9-10:  Column headers (may span two rows for month names)
    Row 11:    Blank separator
    Row 12+:   Data rows  (F12 in Excel = index 11 in 0-based)

Expected 22 columns (approximate):
    Codigo AO | Nombre AO |
    Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic |
    Total Programado

Context rows typically contain:
    UE name/code, Meta code, and Fiscal Year.

Mapped to:
    - One ``ProgramacionMensual`` record per AO × month
      (programado field filled; ejecutado defaults to 0).

Validation rules:
    - Sum of 12 monthly programado values must equal Total column (±1 sol).
    - AO CEPLAN code must be non-empty.
    - Amounts must be non-negative.
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

_MONTH_ABBREVS = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
]
_MONTH_FULL = [
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
]

# Context cell positions (0-based) — typical Formato 5.A layout
_CONTEXT_POSITIONS: dict[str, tuple[int, int]] = {
    "ue_nombre":   (2, 2),
    "ue_codigo":   (2, 5),
    "meta_codigo": (3, 5),
    "anio":        (4, 5),
}

_COL_ALIASES: dict[str, list[str]] = {
    "codigo_ao": [
        "codigo ao", "código ao", "cod ao", "cod. ao",
        "codigo ceplan", "código ceplan", "ceplan",
    ],
    "nombre_ao": [
        "nombre ao", "nombre actividad", "actividad operativa",
        "denominacion", "denominación",
    ],
    "total": [
        "total", "total programado", "total año", "anual",
        "total anual programado",
    ],
}

_CEPLAN_MIN_LEN = 6


def _match_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
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


def _find_month_columns(df: pd.DataFrame) -> list[str | None]:
    """Return a list of 12 column names matching Jan–Dec."""
    cols_lower = [(c.lower().strip(), c) for c in df.columns]
    result: list[str | None] = []
    for abbr, full in zip(_MONTH_ABBREVS, _MONTH_FULL):
        found = None
        for col_lower, col_orig in cols_lower:
            if col_lower.startswith(abbr) or col_lower.startswith(full):
                found = col_orig
                break
        result.append(found)
    return result


class Formato5AParser(BaseParser):
    """Parse Formato 5.A — AO monthly programming (programado only).

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based Excel row index where data begins.
            Defaults to 11 (row 12 in Excel).
    """

    FORMAT_NAME = "FORMATO_5A"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 11,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that the AO code column is present."""
        errors: list[str] = []
        if _match_column(df, _COL_ALIASES["codigo_ao"]) is None:
            errors.append(
                "Formato5A: columna 'codigo_ao' no encontrada. "
                f"Columnas detectadas: {list(df.columns)}"
            )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 5.A parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Extract context from raw header area
        # ----------------------------------------------------------------
        raw_head = self._load_raw_rows(
            sheet_name=self.sheet_name,
            nrows=self.data_start_row,
        )
        context = self._extract_context(raw_head, _CONTEXT_POSITIONS)

        if not context.get("anio"):
            context["anio"] = self._scan_for_value(raw_head, "año")
        if not context.get("meta_codigo"):
            context["meta_codigo"] = self._scan_for_value(raw_head, "meta")
        if not context.get("ue_codigo"):
            context["ue_codigo"] = self._scan_for_value(raw_head, "codigo ue")

        self.result.metadata.update(context)

        try:
            anio = int(float(context.get("anio", "0") or "0"))
        except (ValueError, TypeError):
            anio = 0
            self.result.warnings.append(
                "Formato5A: no se pudo determinar el año; se usará 0."
            )

        ue_codigo = context.get("ue_codigo", "")
        meta_codigo = context.get("meta_codigo", "")

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
            self.result.errors.append("Formato5A: la hoja está vacía.")
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
        col_codigo = _match_column(df, _COL_ALIASES["codigo_ao"])
        col_nombre = _match_column(df, _COL_ALIASES["nombre_ao"])
        col_total = _match_column(df, _COL_ALIASES["total"])
        month_cols = self._resolve_month_columns(df, col_codigo, col_nombre)

        # ----------------------------------------------------------------
        # 6. Iterate data rows
        # ----------------------------------------------------------------
        rows_to_skip = max(0, self.data_start_row - header_row_idx - 1)
        skipped = 0
        valid_rows = 0

        for row_idx, row in df.iterrows():
            if int(row_idx) < rows_to_skip:
                continue
            if self._is_empty_row(row):
                continue
            header_kws = ["codigo", "nombre", "programado", "total"]
            if self._is_header_row(row, header_kws):
                continue

            # ----------------------------------------------------------
            # AO code (required)
            # ----------------------------------------------------------
            raw_code = self._clean_str(row.get(col_codigo, "")) if col_codigo else ""
            codigo_ao = raw_code.upper().strip()

            if not codigo_ao or len(re.sub(r"\s+", "", codigo_ao)) < _CEPLAN_MIN_LEN:
                if codigo_ao:
                    self.result.warnings.append(
                        f"Fila {row_idx}: codigo_ao inválido ('{codigo_ao}') — omitida."
                    )
                skipped += 1
                continue

            nombre_ao = (
                self._clean_str(row.get(col_nombre, "")) if col_nombre else ""
            )

            # ----------------------------------------------------------
            # Monthly programado amounts
            # ----------------------------------------------------------
            monthly: list[float] = []
            for col in month_cols:
                val = self._to_decimal(row.get(col)) if col else 0.0
                monthly.append(max(0.0, val))

            monthly_total = round(sum(monthly), 2)
            declared_total = (
                self._to_decimal(row.get(col_total)) if col_total else monthly_total
            )

            if col_total and abs(monthly_total - declared_total) > 1.0:
                self.result.warnings.append(
                    f"Fila {row_idx} AO '{codigo_ao}': "
                    f"suma mensual ({monthly_total:.2f}) ≠ total declarado "
                    f"({declared_total:.2f})."
                )

            # ----------------------------------------------------------
            # Emit 12 ProgramacionMensual records
            # ----------------------------------------------------------
            for mes_num, programado in enumerate(monthly, start=1):
                self.result.records.append(
                    {
                        "_type": "programacion_mensual",
                        "codigo_ao": codigo_ao,
                        "nombre_ao": nombre_ao,
                        "anio": anio,
                        "ue_codigo": ue_codigo,
                        "meta_codigo": meta_codigo,
                        "mes": mes_num,
                        "programado": round(programado, 2),
                        "ejecutado": 0.0,
                        "saldo": round(programado, 2),
                    }
                )

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
                "meta_codigo": meta_codigo,
            }
        )

        logger.info(
            "Formato5AParser: rows=%d skipped=%d anio=%d ue=%s meta=%s",
            valid_rows,
            skipped,
            anio,
            ue_codigo,
            meta_codigo,
        )
        return self.result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        """Find the 0-based row index that contains 'codigo ao' or 'ceplan'."""
        for r in range(min(12, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if any(kw in row_text for kw in ("codigo ao", "código ao", "ceplan")):
                return r
        return max(0, self.data_start_row - 2)

    def _resolve_month_columns(
        self,
        df: pd.DataFrame,
        col_codigo: str | None,
        col_nombre: str | None,
    ) -> list[str | None]:
        """Return 12 month columns.  Falls back to positional detection."""
        by_name = _find_month_columns(df)
        missing = sum(1 for m in by_name if m is None)
        if missing == 0:
            return by_name

        # Positional fallback: columns after nombre_ao
        ref_col = col_nombre or col_codigo
        if ref_col:
            all_cols = list(df.columns)
            try:
                ref_idx = all_cols.index(ref_col)
                candidates = all_cols[ref_idx + 1 : ref_idx + 13]
                if len(candidates) == 12:
                    logger.info(
                        "Formato5AParser: using positional month columns "
                        "after '%s'",
                        ref_col,
                    )
                    return candidates
            except (ValueError, IndexError):
                pass

        logger.warning(
            "Formato5AParser: %d month columns not resolved — "
            "monthly records may be incomplete.",
            missing,
        )
        return by_name
