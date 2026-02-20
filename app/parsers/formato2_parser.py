"""Parser for Formato 2 — Task-level Budget Programming.

Sheet layout (rows are 1-based as in Excel):
    Row 1: Title
    Row 2: Blank or subtitle
    Row 3: UE name / code context
    Row 4: Meta context
    Row 5: Year context
    Row 6: Blank / separator
    Row 7: Column headers
    Row 8+: Data rows (F8 in Excel = index 7 in 0-based)

Expected 19 columns (approximate):
    Cod Meta | Desc Meta | Cod AO | Desc AO | Cod Tarea | Desc Tarea |
    Clasificador | Desc Clasificador | PIM |
    Ene | Feb | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct | Nov | Dic

Mapped to:
    - One ``ProgramacionPresupuestal`` record per data row
      (fields: anio, ue_codigo, meta_codigo, ao_codigo, tarea_codigo,
      clasificador_codigo, pim and monthly breakdown).

Validation rules:
    - Clasificador code must match the ``X.X…`` pattern.
    - PIM must be non-negative.
    - Sum of 12 monthly amounts must equal PIM (tolerance ±1 sol).
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
_CONTEXT_POSITIONS: dict[str, tuple[int, int]] = {
    "ue_nombre":   (2, 1),
    "ue_codigo":   (2, 3),
    "meta_codigo": (3, 3),
    "anio":        (4, 3),
}

_CLASIFICADOR_RE = re.compile(r"^\d+(\.\d+){1,5}$")

_COL_ALIASES: dict[str, list[str]] = {
    "cod_meta": [
        "cod meta", "código meta", "codigo meta", "cod. meta",
        "meta codigo", "meta código",
    ],
    "desc_meta": [
        "desc meta", "descripcion meta", "descripción meta",
        "denominacion meta", "nombre meta",
    ],
    "cod_ao": [
        "cod ao", "código ao", "codigo ao", "cod. ao",
        "codigo ceplan", "ceplan",
    ],
    "desc_ao": [
        "desc ao", "descripcion ao", "descripción ao",
        "nombre ao", "actividad operativa", "denominacion ao",
    ],
    "cod_tarea": [
        "cod tarea", "código tarea", "codigo tarea", "cod. tarea",
        "tarea codigo", "tarea código",
    ],
    "desc_tarea": [
        "desc tarea", "descripcion tarea", "descripción tarea",
        "denominacion tarea", "nombre tarea", "tarea",
    ],
    "clasificador": [
        "clasificador", "código", "codigo", "cod. gasto", "cod gasto",
        "clasificador de gasto",
    ],
    "desc_clasificador": [
        "desc clasificador", "descripcion clasificador",
        "descripción clasificador", "descripcion del gasto",
        "denominacion clasificador",
    ],
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

    missing = sum(1 for m in result if m is None)
    if missing:
        logger.warning(
            "Formato2Parser: %d month columns not found by name; "
            "will try positional detection.",
            missing,
        )
    return result


class Formato2Parser(BaseParser):
    """Parse Formato 2 — task-level budget programming with monthly breakdown.

    Formato 2 extends Formato 1 by adding a task hierarchy dimension:
    each row belongs to a Meta → AO → Tarea chain in addition to the
    budget classifier code.  Monthly programming amounts per classifier
    per task are captured as ``ProgramacionPresupuestal`` records.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 7 (row 8 in Excel).
        context_start_row: 0-based first row of context/header area.
            Defaults to 0.
    """

    FORMAT_NAME = "FORMATO_2"

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
        """Verify that required columns are present."""
        errors: list[str] = []
        required_aliases = {
            "clasificador": _COL_ALIASES["clasificador"],
            "pim":          _COL_ALIASES["pim"],
            "cod_tarea":    _COL_ALIASES["cod_tarea"],
        }
        for field, aliases in required_aliases.items():
            if _match_column(df, aliases) is None:
                errors.append(
                    f"Formato2: columna '{field}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 2 parsing pipeline."""
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
            context["anio"] = self._scan_for_value(raw_head, "año")
        if not context.get("meta_codigo"):
            context["meta_codigo"] = self._scan_for_value(raw_head, "meta")
        if not context.get("ue_nombre"):
            context["ue_nombre"] = self._scan_for_value(raw_head, "unidad ejecutora")
        if not context.get("ue_codigo"):
            context["ue_codigo"] = self._scan_for_value(
                raw_head, "codigo ue", col_offset=1
            )

        self.result.metadata.update(context)

        try:
            anio = int(float(context.get("anio", "0")))
        except (ValueError, TypeError):
            anio = 0
            self.result.warnings.append(
                "Formato2: no se pudo determinar el año del contexto; se usará 0."
            )

        ue_codigo = context.get("ue_codigo", "")
        meta_codigo = context.get("meta_codigo", "")

        # ----------------------------------------------------------------
        # 2. Detect actual header row
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
            self.result.errors.append("Formato2: la hoja está vacía.")
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
        col_cod_meta   = _match_column(df, _COL_ALIASES["cod_meta"])
        col_desc_meta  = _match_column(df, _COL_ALIASES["desc_meta"])
        col_cod_ao     = _match_column(df, _COL_ALIASES["cod_ao"])
        col_desc_ao    = _match_column(df, _COL_ALIASES["desc_ao"])
        col_cod_tarea  = _match_column(df, _COL_ALIASES["cod_tarea"])
        col_desc_tarea = _match_column(df, _COL_ALIASES["desc_tarea"])
        col_clas       = _match_column(df, _COL_ALIASES["clasificador"])
        col_desc_clas  = _match_column(df, _COL_ALIASES["desc_clasificador"])
        col_pim        = _match_column(df, _COL_ALIASES["pim"])
        col_total      = _match_column(df, _COL_ALIASES["total"])
        month_cols     = self._resolve_month_columns(df)

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
            header_kws = ["clasificador", "pim", "tarea", "meta"]
            if self._is_header_row(row, header_kws):
                continue

            # ----------------------------------------------------------
            # Extract classifier code (required)
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

            # ----------------------------------------------------------
            # Hierarchy codes
            # ----------------------------------------------------------
            cod_meta  = self._clean_str(row.get(col_cod_meta, ""))  if col_cod_meta  else ""
            desc_meta = self._clean_str(row.get(col_desc_meta, "")) if col_desc_meta else ""
            cod_ao    = self._clean_str(row.get(col_cod_ao, ""))    if col_cod_ao    else ""
            desc_ao   = self._clean_str(row.get(col_desc_ao, ""))   if col_desc_ao   else ""
            cod_tarea  = self._clean_str(row.get(col_cod_tarea, ""))  if col_cod_tarea  else ""
            desc_tarea = self._clean_str(row.get(col_desc_tarea, "")) if col_desc_tarea else ""
            desc_clas  = self._clean_str(row.get(col_desc_clas, ""))  if col_desc_clas  else ""

            # Use context meta if row-level meta is absent
            effective_meta = cod_meta or meta_codigo

            pim = self._to_decimal(row.get(col_pim)) if col_pim else 0.0

            if pim < 0:
                self.result.warnings.append(
                    f"Fila {row_idx}: PIM negativo ({pim:.2f}) "
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
                monthly.append(max(0.0, val))

            monthly_total = round(sum(monthly), 2)
            declared_total = (
                self._to_decimal(row.get(col_total)) if col_total else monthly_total
            )

            # Validate sum ≈ PIM (or declared total)
            reference = declared_total if col_total else pim
            if abs(monthly_total - reference) > 1.0:
                self.result.warnings.append(
                    f"Fila {row_idx} clasificador '{clasificador}': "
                    f"suma mensual ({monthly_total:.2f}) ≠ referencia "
                    f"({reference:.2f})."
                )

            # ----------------------------------------------------------
            # Emit ProgramacionPresupuestal record
            # ----------------------------------------------------------
            pp_record: dict[str, Any] = {
                "_type": "programacion_presupuestal",
                "anio": anio,
                "ue_codigo": ue_codigo,
                "meta_codigo": effective_meta,
                "desc_meta": desc_meta,
                "ao_codigo": cod_ao,
                "desc_ao": desc_ao,
                "tarea_codigo": cod_tarea,
                "desc_tarea": desc_tarea,
                "clasificador_codigo": clasificador,
                "descripcion": desc_clas,
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
                        "meta_codigo": effective_meta,
                        "ao_codigo": cod_ao,
                        "tarea_codigo": cod_tarea,
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
            "Formato2Parser: rows=%d skipped=%d anio=%d ue=%s meta=%s",
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
        """Find the 0-based row index containing 'clasificador' or 'tarea'."""
        for r in range(min(10, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if "clasificador" in row_text or "tarea" in row_text:
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
                        "Formato2Parser: using positional month columns "
                        "starting after '%s'",
                        col_pim,
                    )
                    return [c for c in month_candidates]
            except (ValueError, IndexError):
                pass

        logger.warning(
            "Formato2Parser: could not resolve all 12 month columns — "
            "%d missing.  Monthly records may be incomplete.",
            missing_count,
        )
        return by_name
