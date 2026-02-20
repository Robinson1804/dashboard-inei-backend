"""Parser for Formato 1 — Annual Budget Programming (PIA / PIM + 12 months).

Sheet layout (rows are 1-based as in Excel):
    Row 1: Title
    Row 2: Blank or subtitle
    Row 3: UE name / code context
    Row 4: Meta context
    Row 5: Year context
    Row 6: Blank / separator
    Row 7: Column headers
    Row 8+: Data rows (F8 in Excel = index 7 in 0-based)

Expected 23 columns (approximate):
    Clasificador | Descripcion | PIA | PIM |
    Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic |
    Total | [optional extra columns]

Mapped to:
    - One ``ProgramacionPresupuestal`` record per data row
      (fields: anio, ue_codigo, meta_codigo, clasificador_codigo, pia, pim)
    - Twelve ``ProgramacionMensual`` records per data row
      (fields: mes, programado — ejecutado defaults to 0)

Validation rules:
    - Sum of 12 monthly amounts must equal the Total column
      (tolerance: ±1 due to rounding).
    - Amounts must be non-negative.
    - Classifier code must match the "X.X…" pattern.
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

_MONTH_NAMES_ES = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
]
_MONTH_NAMES_FULL = [
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
]

# Context cell positions (0-based row, 0-based col) in the raw header area.
# Row 2 = "Unidad Ejecutora: xxx" (Excel row 2, 0-based row 1)
# Row 3 = "Meta Presupuestal: xxx" (Excel row 3, 0-based row 2)
# Row 4 = "Ano Fiscal: xxx" (Excel row 4, 0-based row 3)
_CONTEXT_POSITIONS: dict[str, tuple[int, int]] = {
    "ue_nombre":    (1, 1),   # Row 2 col B
    "meta_codigo":  (2, 1),   # Row 3 col B
    "anio":         (3, 1),   # Row 4 col B
}

_CLASIFICADOR_RE = re.compile(r"^\d+(\.\d+){1,5}$")

# Approximate column layout aliases
_COL_ALIASES: dict[str, list[str]] = {
    "clasificador": [
        "clasificador", "código", "codigo", "cod. gasto", "cod gasto",
        "clasificador de gasto",
    ],
    "descripcion": [
        "descripcion", "descripción", "nombre", "denominacion",
        "descripcion del gasto",
    ],
    "pia": ["pia", "presupuesto institucional de apertura"],
    "pim": ["pim", "presupuesto institucional modificado"],
    "total": ["total", "total año", "total anual", "anual"],
}


def _match_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Find the first DataFrame column matching any alias (case-insensitive)."""
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
    """Return a list of 12 column names matching Jan–Dec, in order.

    Tries abbreviation matching first, then positional fallback.
    """
    cols_lower = [(c.lower().strip(), c) for c in df.columns]
    result: list[str | None] = []

    for abbr, full in zip(_MONTH_NAMES_ES, _MONTH_NAMES_FULL):
        found = None
        for col_lower, col_orig in cols_lower:
            if col_lower.startswith(abbr) or col_lower.startswith(full):
                found = col_orig
                break
        result.append(found)

    # If we couldn't find by name, log a warning — caller handles None
    missing = sum(1 for m in result if m is None)
    if missing:
        logger.warning(
            "Formato1Parser: %d month columns not found by name; "
            "will try positional detection.",
            missing,
        )
    return result


class Formato1Parser(BaseParser):
    """Parse Formato 1 — annual budget programming with monthly breakdown.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 7 (row 8 in Excel).
        context_start_row: 0-based first row of context/header area.
            Defaults to 0.
    """

    FORMAT_NAME = "FORMATO_1"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 7,
        context_start_row: int = 0,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row
        self.context_start_row = context_start_row

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify required columns exist."""
        errors: list[str] = []
        required_aliases = {
            "clasificador": _COL_ALIASES["clasificador"],
            "descripcion": _COL_ALIASES["descripcion"],
            "pia": _COL_ALIASES["pia"],
            "pim": _COL_ALIASES["pim"],
        }
        for field, aliases in required_aliases.items():
            if _match_column(df, aliases) is None:
                errors.append(
                    f"Formato1: columna '{field}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 1 parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Extract context metadata from header rows
        # ----------------------------------------------------------------
        raw_head = self._load_raw_rows(
            sheet_name=self.sheet_name,
            nrows=self.data_start_row,
        )
        context = self._extract_context(raw_head, _CONTEXT_POSITIONS)

        # Fallback: scan for labels
        if not context.get("anio"):
            context["anio"] = (
                self._scan_for_value(raw_head, "año")
                or self._scan_for_value(raw_head, "ano")
            )
        if not context.get("meta_codigo"):
            context["meta_codigo"] = (
                self._scan_for_value(raw_head, "meta presupuestal")
                or self._scan_for_value(raw_head, "meta")
            )
        if not context.get("ue_nombre"):
            context["ue_nombre"] = (
                self._scan_for_value(raw_head, "unidad ejecutora")
            )
        if not context.get("ue_codigo"):
            # Try explicit "codigo ue" label first
            context["ue_codigo"] = self._scan_for_value(raw_head, "codigo ue", col_offset=1)
            # Fallback: extract code from ue_nombre "001 - INEI SEDE CENTRAL" → "001"
            if not context["ue_codigo"] and context.get("ue_nombre"):
                ue_text = context["ue_nombre"].strip()
                if " - " in ue_text:
                    context["ue_codigo"] = ue_text.split(" - ")[0].strip()
                elif re.match(r"^\d{3}", ue_text):
                    context["ue_codigo"] = ue_text[:3]

        self.result.metadata.update(context)

        try:
            anio = int(float(context.get("anio", "0")))
        except (ValueError, TypeError):
            anio = 0
            self.result.warnings.append(
                "Formato1: no se pudo determinar el año del contexto; se usará 0."
            )

        ue_codigo = context.get("ue_codigo", "")
        meta_codigo = context.get("meta_codigo", "")

        # ----------------------------------------------------------------
        # 2. Detect actual header row (the row with "PIA", "PIM", etc.)
        # ----------------------------------------------------------------
        header_row_idx = self._detect_header_row(raw_head)

        # ----------------------------------------------------------------
        # 3. Load data area
        # ----------------------------------------------------------------
        df = self._load_sheet(
            sheet_name=self.sheet_name,
            header=header_row_idx,
            dtype=str,
        )
        if df.empty:
            self.result.errors.append("Formato1: la hoja está vacía.")
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
        col_pia = _match_column(df, _COL_ALIASES["pia"])
        col_pim = _match_column(df, _COL_ALIASES["pim"])
        col_total = _match_column(df, _COL_ALIASES["total"])
        month_cols = self._resolve_month_columns(df)

        # ----------------------------------------------------------------
        # 6. Iterate data rows
        # ----------------------------------------------------------------
        skipped = 0
        valid_rows = 0

        # Skip rows before data_start_row relative to the header
        rows_to_skip = max(0, self.data_start_row - header_row_idx - 1)

        for row_idx, row in df.iterrows():
            # Skip header-area rows that ended up in the dataframe
            if int(row_idx) < rows_to_skip:
                continue

            if self._is_empty_row(row):
                continue

            header_kws = ["clasificador", "pia", "pim", "descripcion"]
            if self._is_header_row(row, header_kws):
                continue

            # ----------------------------------------------------------
            # Extract classifier code
            # ----------------------------------------------------------
            raw_clas = self._clean_str(row.get(col_clas, "")) if col_clas else ""
            clasificador = self._normalize_clasificador(raw_clas)

            if not clasificador or not _CLASIFICADOR_RE.match(clasificador):
                if clasificador:
                    self.result.warnings.append(
                        f"Fila {row_idx}: código clasificador inválido "
                        f"('{clasificador}') — fila omitida."
                    )
                skipped += 1
                continue

            descripcion = self._clean_str(row.get(col_desc, "")) if col_desc else ""
            pia = self._to_decimal(row.get(col_pia)) if col_pia else 0.0
            pim = self._to_decimal(row.get(col_pim)) if col_pim else 0.0

            # Negative amounts are invalid
            if pia < 0 or pim < 0:
                self.result.warnings.append(
                    f"Fila {row_idx}: montos negativos (PIA={pia}, PIM={pim}) "
                    f"para clasificador '{clasificador}' — fila omitida."
                )
                skipped += 1
                continue

            # ----------------------------------------------------------
            # Extract 12 monthly amounts
            # ----------------------------------------------------------
            monthly: list[float] = []
            for col in month_cols:
                val = self._to_decimal(row.get(col)) if col else 0.0
                monthly.append(max(0.0, val))  # clamp negatives to 0

            monthly_total = round(sum(monthly), 2)
            declared_total = (
                self._to_decimal(row.get(col_total)) if col_total else monthly_total
            )

            # Validate sum ≈ total (tolerance ±1 sol)
            if col_total and abs(monthly_total - declared_total) > 1.0:
                self.result.warnings.append(
                    f"Fila {row_idx} clasificador '{clasificador}': "
                    f"suma mensual ({monthly_total:.2f}) ≠ total declarado "
                    f"({declared_total:.2f})."
                )

            # ----------------------------------------------------------
            # Emit ProgramacionPresupuestal record
            # ----------------------------------------------------------
            pp_record: dict[str, Any] = {
                "_type": "programacion_presupuestal",
                "anio": anio,
                "ue_codigo": ue_codigo,
                "meta_codigo": meta_codigo,
                "clasificador_codigo": clasificador,
                "descripcion": descripcion,
                "pia": round(pia, 2),
                "pim": round(pim, 2),
                "certificado": 0.0,
                "compromiso_anual": 0.0,
                "devengado": 0.0,
                "girado": 0.0,
                "saldo": round(pim, 2),
            }
            self.result.records.append(pp_record)

            # ----------------------------------------------------------
            # Emit 12 ProgramacionMensual records
            # ----------------------------------------------------------
            for mes_num, programado in enumerate(monthly, start=1):
                self.result.records.append(
                    {
                        "_type": "programacion_mensual",
                        "clasificador_codigo": clasificador,
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
            "Formato1Parser: rows=%d skipped=%d anio=%d ue=%s meta=%s",
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
        """Find the 0-based row index containing 'PIA' or 'PIM'."""
        for r in range(min(10, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if "pia" in row_text or "pim" in row_text:
                return r
        return max(0, self.data_start_row - 1)

    def _resolve_month_columns(self, df: pd.DataFrame) -> list[str | None]:
        """Return 12 column names (Jan–Dec).  Falls back to positional logic."""
        by_name = _find_month_columns(df)
        missing_count = sum(1 for m in by_name if m is None)

        if missing_count == 0:
            return by_name

        # Positional fallback: find PIM column and take the next 12 numeric cols
        col_pim = _match_column(df, _COL_ALIASES["pim"])
        if col_pim:
            all_cols = list(df.columns)
            try:
                pim_idx = all_cols.index(col_pim)
                month_candidates = all_cols[pim_idx + 1 : pim_idx + 13]
                if len(month_candidates) == 12:
                    logger.info(
                        "Formato1Parser: using positional month columns "
                        "starting after '%s'",
                        col_pim,
                    )
                    return [c for c in month_candidates]
            except (ValueError, IndexError):
                pass

        logger.warning(
            "Formato1Parser: could not resolve all 12 month columns — "
            "%d missing.  Monthly records may be incomplete.",
            missing_count,
        )
        return by_name
