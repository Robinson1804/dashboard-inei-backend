"""Parser for Formato 5 Resumen — AO Execution Summary.

Sheet layout (rows are 1-based as in Excel):
    Rows 1–4:  Title block and institutional context
    Row 5:     Blank / separator
    Row 6:     Column headers
    Row 7+:    Data rows (F7 in Excel = index 6 in 0-based)

Expected 20 columns (approximate):
    Codigo AO | Nombre AO |
    PIM | CCP | Compromiso Anual | Devengado | Girado | Saldo |
    % Avance PIM | % Avance CCP | Semaforo |
    Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic

Context rows typically contain:
    UE name/code, Meta code, and Fiscal Year (same positions as Formato 5.A).

Mapped to:
    - One ``ao_resumen`` record per AO row containing the full financial
      summary (PIM, CCP, compromiso, devengado, girado, saldo, semaforo,
      % avance) plus the 12 monthly devengado amounts.

Validation rules:
    - codigo_ao must be non-empty and at least 6 characters long.
    - All financial amounts must be non-negative.
    - Sum of 12 monthly devengado amounts must equal devengado annual
      total (tolerance ±1 sol).
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

# Context cell positions (0-based) — same layout as Formato 5.A
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
    "pim": ["pim", "presupuesto institucional modificado"],
    "ccp": [
        "ccp", "certificado credito presupuestario",
        "certificado de crédito presupuestario",
        "certificado crédito", "certificacion",
    ],
    "compromiso_anual": [
        "compromiso anual", "compromiso", "comp. anual",
        "compromiso anu.", "c. anual",
    ],
    "devengado": [
        "devengado", "devengados", "devengado anual",
        "monto devengado",
    ],
    "girado": ["girado", "girados", "pagado", "pagados"],
    "saldo": [
        "saldo", "saldo disponible", "saldo por ejecutar",
        "saldo pim",
    ],
    "pct_avance_pim": [
        "% avance pim", "% ejec pim", "avance pim",
        "porcentaje pim", "% pim",
    ],
    "pct_avance_ccp": [
        "% avance ccp", "% ejec ccp", "avance ccp",
        "porcentaje ccp", "% ccp",
    ],
    "semaforo": [
        "semaforo", "semáforo", "estado", "color", "semaf.",
    ],
    "total": [
        "total", "total devengado", "total año", "anual",
        "total anual devengado",
    ],
}

_CEPLAN_MIN_LEN = 6


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
    """Return a list of 12 column names matching Jan–Dec, in order."""
    cols_lower = [(c.lower().strip(), c) for c in df.columns]
    result: list[str | None] = []

    for abbr, full in zip(_MONTH_ABBREVS, _MONTH_FULL):
        found = None
        for col_lower, col_orig in cols_lower:
            if col_lower.startswith(abbr) or col_lower.startswith(full):
                found = col_orig
                break
        result.append(found)

    missing = sum(1 for m in result if m is None)
    if missing:
        logger.warning(
            "Formato5ResumenParser: %d month columns not found by name; "
            "will try positional detection.",
            missing,
        )
    return result


class Formato5ResumenParser(BaseParser):
    """Parse Formato 5 Resumen — consolidated AO execution summary.

    This format provides a single-row-per-AO summary view with all key
    budget execution indicators (PIM, CCP, devengado, girado, saldo,
    semaforo) plus a 12-month devengado breakdown.  It is typically used
    for management reporting across all 85 AOs in a given UE/Meta.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 6 (row 7 in Excel).
    """

    FORMAT_NAME = "FORMATO_5_RESUMEN"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 6,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that the AO code column and at least devengado are present."""
        errors: list[str] = []
        required_aliases = {
            "codigo_ao": _COL_ALIASES["codigo_ao"],
            "devengado": _COL_ALIASES["devengado"],
        }
        for field, aliases in required_aliases.items():
            if _match_column(df, aliases) is None:
                errors.append(
                    f"Formato5Resumen: columna '{field}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 5 Resumen parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Extract context metadata from header rows
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
                "Formato5Resumen: no se pudo determinar el año; se usará 0."
            )

        ue_codigo  = context.get("ue_codigo", "")
        meta_codigo = context.get("meta_codigo", "")

        # ----------------------------------------------------------------
        # 2. Detect header row
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
            self.result.errors.append("Formato5Resumen: la hoja está vacía.")
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
        col_codigo        = _match_column(df, _COL_ALIASES["codigo_ao"])
        col_nombre        = _match_column(df, _COL_ALIASES["nombre_ao"])
        col_pim           = _match_column(df, _COL_ALIASES["pim"])
        col_ccp           = _match_column(df, _COL_ALIASES["ccp"])
        col_compromiso    = _match_column(df, _COL_ALIASES["compromiso_anual"])
        col_devengado     = _match_column(df, _COL_ALIASES["devengado"])
        col_girado        = _match_column(df, _COL_ALIASES["girado"])
        col_saldo         = _match_column(df, _COL_ALIASES["saldo"])
        col_pct_pim       = _match_column(df, _COL_ALIASES["pct_avance_pim"])
        col_pct_ccp       = _match_column(df, _COL_ALIASES["pct_avance_ccp"])
        col_semaforo      = _match_column(df, _COL_ALIASES["semaforo"])
        col_total         = _match_column(df, _COL_ALIASES["total"])
        month_cols        = self._resolve_month_columns(df, col_codigo, col_nombre)

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
            header_kws = ["codigo ao", "devengado", "semaforo", "pim"]
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

            nombre_ao = self._clean_str(row.get(col_nombre, "")) if col_nombre else ""

            # ----------------------------------------------------------
            # Financial amounts
            # ----------------------------------------------------------
            pim            = self._to_decimal(row.get(col_pim))        if col_pim        else 0.0
            ccp            = self._to_decimal(row.get(col_ccp))        if col_ccp        else 0.0
            compromiso     = self._to_decimal(row.get(col_compromiso)) if col_compromiso else 0.0
            devengado      = self._to_decimal(row.get(col_devengado))  if col_devengado  else 0.0
            girado         = self._to_decimal(row.get(col_girado))     if col_girado     else 0.0
            saldo          = self._to_decimal(row.get(col_saldo))      if col_saldo      else None
            pct_avance_pim = self._to_decimal(row.get(col_pct_pim))   if col_pct_pim    else None
            pct_avance_ccp = self._to_decimal(row.get(col_pct_ccp))   if col_pct_ccp    else None
            semaforo       = self._clean_str(row.get(col_semaforo, "")) if col_semaforo  else ""

            # Clamp negatives
            for amount_name, amount in (
                ("pim", pim), ("ccp", ccp), ("compromiso", compromiso),
                ("devengado", devengado), ("girado", girado),
            ):
                if amount < 0:
                    self.result.warnings.append(
                        f"Fila {row_idx} AO '{codigo_ao}': "
                        f"monto negativo en '{amount_name}' ({amount:.2f}) — "
                        "se usará 0."
                    )
            pim        = max(0.0, pim)
            ccp        = max(0.0, ccp)
            compromiso = max(0.0, compromiso)
            devengado  = max(0.0, devengado)
            girado     = max(0.0, girado)

            computed_saldo = round(pim - devengado, 2)
            saldo_final = saldo if saldo is not None else computed_saldo

            # ----------------------------------------------------------
            # Monthly amounts
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
                    f"suma mensual devengado ({monthly_total:.2f}) ≠ total declarado "
                    f"({declared_total:.2f})."
                )

            # ----------------------------------------------------------
            # Emit ao_resumen record
            # ----------------------------------------------------------
            record: dict[str, Any] = {
                "_type": "ao_resumen",
                "anio": anio,
                "ue_codigo": ue_codigo,
                "meta_codigo": meta_codigo,
                "codigo_ao": codigo_ao,
                "nombre_ao": nombre_ao,
                "pim": round(pim, 2),
                "ccp": round(ccp, 2),
                "compromiso_anual": round(compromiso, 2),
                "devengado": round(devengado, 2),
                "girado": round(girado, 2),
                "saldo": round(saldo_final, 2),
                "pct_avance_pim": (
                    round(pct_avance_pim, 4) if pct_avance_pim is not None else None
                ),
                "pct_avance_ccp": (
                    round(pct_avance_ccp, 4) if pct_avance_ccp is not None else None
                ),
                "semaforo": semaforo,
                "devengado_mensual": {
                    str(mes): round(amt, 2)
                    for mes, amt in enumerate(monthly, start=1)
                },
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
                "meta_codigo": meta_codigo,
            }
        )

        logger.info(
            "Formato5ResumenParser: rows=%d skipped=%d anio=%d ue=%s meta=%s",
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
        """Find the 0-based row containing 'codigo ao', 'devengado', or 'semaforo'."""
        for r in range(min(12, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if any(
                kw in row_text
                for kw in ("codigo ao", "código ao", "devengado", "semaforo", "semáforo")
            ):
                return r
        return max(0, self.data_start_row - 1)

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

        # Positional fallback: columns after semaforo or nombre_ao
        ref_col = _match_column(df, _COL_ALIASES["semaforo"]) or col_nombre or col_codigo
        if ref_col:
            all_cols = list(df.columns)
            try:
                ref_idx = all_cols.index(ref_col)
                candidates = all_cols[ref_idx + 1 : ref_idx + 13]
                if len(candidates) == 12:
                    logger.info(
                        "Formato5ResumenParser: using positional month columns "
                        "after '%s'",
                        ref_col,
                    )
                    return candidates
            except (ValueError, IndexError):
                pass

        logger.warning(
            "Formato5ResumenParser: %d month columns not resolved — "
            "monthly breakdown may be incomplete.",
            missing,
        )
        return by_name
