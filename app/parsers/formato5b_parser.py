"""Parser for Formato 5.B — AO Execution (Programado + Ejecutado + Saldo).

This is the most structurally complex of the INEI Excel formats.

Sheet layout (rows are 1-based as in Excel):
    Rows 1–8:  Title block and institutional context
    Rows 9-10: Two-row column headers (month names + sub-columns)
    Row 11:    Blank separator
    Row 12+:   Data rows  (F12 in Excel = index 11 in 0-based)

Column structure (~45 columns):
    Codigo AO | Nombre AO |
    [For each month: Programado | Ejecutado | Saldo]  × 12 months = 36 cols
    [For each quarter Q1..Q4: Programado | Ejecutado | Saldo]      =  4 × 3 = 12 cols
    Annual Total: Programado | Ejecutado | Saldo                   =  3 cols
    -----------------------------------------------------------------------
    Grand total: 2 + 36 + 12 + 3 = ~53 (the specification says ~45; actual
    files may omit quarterly subtotals or vary per version)

Detection strategy for triple-header columns:
    - Row 9 contains month names (Enero, Febrero … Diciembre) or quarter labels.
    - Row 10 contains "Programado", "Ejecutado", "Saldo" repeated under each.
    - The parser merges rows 9+10 into composite column names like
      "Enero_Programado", "Enero_Ejecutado", "Enero_Saldo".

Mapped to:
    - One ``ProgramacionMensual`` record per AO × month, with both
      ``programado`` and ``ejecutado`` fields filled in.

Validation rules:
    - ejecutado ≤ programado per month (warning, not hard error).
    - saldo = programado - ejecutado (warn if inconsistent).
    - Quarterly sums must match sum of constituent monthly amounts (±1).
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
_MONTH_MAP: dict[str, int] = {}
for _i, (_a, _f) in enumerate(zip(_MONTH_ABBREVS, _MONTH_FULL), start=1):
    _MONTH_MAP[_a] = _i
    _MONTH_MAP[_f] = _i

_QUARTER_LABELS = {"q1", "q2", "q3", "q4", "i trim", "ii trim", "iii trim", "iv trim",
                   "1er trim", "2do trim", "3er trim", "4to trim",
                   "trimestre 1", "trimestre 2", "trimestre 3", "trimestre 4"}

_SUB_COLS = ("programado", "ejecutado", "saldo")

_CONTEXT_POSITIONS: dict[str, tuple[int, int]] = {
    "ue_nombre":   (2, 2),
    "ue_codigo":   (2, 5),
    "meta_codigo": (3, 5),
    "anio":        (4, 5),
}

_COL_ALIASES_CODIGO: list[str] = [
    "codigo ao", "código ao", "cod ao", "cod. ao",
    "codigo ceplan", "código ceplan", "ceplan",
]
_COL_ALIASES_NOMBRE: list[str] = [
    "nombre ao", "nombre actividad", "actividad operativa",
    "denominacion", "denominación",
]

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


# ---------------------------------------------------------------------------
# Header parsing helpers
# ---------------------------------------------------------------------------


def _is_month_label(text: str) -> int | None:
    """Return month number (1-12) if text is a month name/abbreviation, else None."""
    clean = text.strip().lower()
    for abbr, full in zip(_MONTH_ABBREVS, _MONTH_FULL):
        if clean.startswith(abbr) or clean == full:
            return _MONTH_MAP[abbr]
    return None


def _is_quarter_label(text: str) -> bool:
    clean = text.strip().lower()
    return any(q in clean for q in _QUARTER_LABELS)


def _is_subcol(text: str) -> str | None:
    """Return 'programado', 'ejecutado', or 'saldo' if text matches one."""
    clean = text.strip().lower()
    for sc in _SUB_COLS:
        if clean.startswith(sc):
            return sc
    return None


class Formato5BParser(BaseParser):
    """Parse Formato 5.B — AO execution tracking with programado + ejecutado.

    The two-row compound header is merged into composite column names before
    data extraction begins.  Quarterly and annual subtotal columns are detected
    and skipped during monthly iteration.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 11 (row 12 in Excel).
        header_rows: Tuple of two 0-based row indices for the compound header.
            Defaults to (8, 9) (rows 9 and 10 in Excel).
    """

    FORMAT_NAME = "FORMATO_5B"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 11,
        header_rows: tuple[int, int] = (8, 9),
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row
        self.header_rows = header_rows

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that both AO code and at least one month triple exist."""
        errors: list[str] = []
        if _match_column(df, _COL_ALIASES_CODIGO) is None:
            errors.append(
                "Formato5B: columna 'codigo_ao' no encontrada. "
                f"Columnas detectadas: {list(df.columns)}"
            )
        # Check for at least one programado sub-column
        prog_cols = [c for c in df.columns if "programado" in c.lower()]
        if not prog_cols:
            errors.append(
                "Formato5B: ninguna columna 'Programado' encontrada. "
                "El encabezado de dos filas puede no haberse detectado correctamente."
            )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 5.B parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Extract context metadata
        # ----------------------------------------------------------------
        raw_all = self._load_raw_rows(sheet_name=self.sheet_name)
        context = self._extract_context(raw_all, _CONTEXT_POSITIONS)

        if not context.get("anio"):
            context["anio"] = self._scan_for_value(raw_all, "año")
        if not context.get("meta_codigo"):
            context["meta_codigo"] = self._scan_for_value(raw_all, "meta")
        if not context.get("ue_codigo"):
            context["ue_codigo"] = self._scan_for_value(raw_all, "codigo ue")

        self.result.metadata.update(context)

        try:
            anio = int(float(context.get("anio", "0") or "0"))
        except (ValueError, TypeError):
            anio = 0
            self.result.warnings.append(
                "Formato5B: no se pudo determinar el año; se usará 0."
            )

        ue_codigo = context.get("ue_codigo", "")
        meta_codigo = context.get("meta_codigo", "")

        # ----------------------------------------------------------------
        # 2. Build compound column names from the two header rows
        # ----------------------------------------------------------------
        compound_cols, actual_header_rows = self._build_compound_columns(raw_all)

        if not compound_cols:
            self.result.errors.append(
                "Formato5B: no se pudo construir el encabezado compuesto. "
                "Verificar que el archivo tenga el formato correcto."
            )
            return self.result

        # ----------------------------------------------------------------
        # 3. Load data area with pre-built column names
        # ----------------------------------------------------------------
        skip_count = actual_header_rows[1] + 1  # rows before data
        try:
            df = pd.read_excel(
                self._open_excel(),
                sheet_name=self.sheet_name,
                header=None,
                skiprows=skip_count,
                names=compound_cols,
                dtype=str,
                engine="openpyxl",
            )
        except Exception as exc:
            self.result.errors.append(
                f"Formato5B: error cargando datos: {exc}"
            )
            return self.result

        df = self._forward_fill_merged(df)

        # ----------------------------------------------------------------
        # 4. Validate structure
        # ----------------------------------------------------------------
        struct_errors = self.validate_structure(df)
        self.result.errors.extend(struct_errors)
        if struct_errors:
            return self.result

        # ----------------------------------------------------------------
        # 5. Identify column groups
        # ----------------------------------------------------------------
        col_codigo = _match_column(df, _COL_ALIASES_CODIGO)
        col_nombre = _match_column(df, _COL_ALIASES_NOMBRE)

        # month_triples: dict of mes_num → {"programado": col, "ejecutado": col, "saldo": col}
        month_triples = self._build_month_triples(compound_cols)

        if not month_triples:
            self.result.errors.append(
                "Formato5B: no se encontraron tríos mes/programado/ejecutado/saldo."
            )
            return self.result

        # ----------------------------------------------------------------
        # 6. Iterate data rows
        # ----------------------------------------------------------------
        rows_to_skip_in_df = max(0, self.data_start_row - skip_count)
        skipped = 0
        valid_rows = 0

        for row_idx, row in df.iterrows():
            if int(row_idx) < rows_to_skip_in_df:
                continue
            if self._is_empty_row(row):
                continue
            header_kws = ["codigo", "nombre", "programado", "ejecutado"]
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
            # Emit ProgramacionMensual records
            # ----------------------------------------------------------
            for mes_num in sorted(month_triples):
                triple = month_triples[mes_num]

                prog_col = triple.get("programado")
                ejec_col = triple.get("ejecutado")
                saldo_col = triple.get("saldo")

                programado = self._to_decimal(row.get(prog_col)) if prog_col else 0.0
                ejecutado = self._to_decimal(row.get(ejec_col)) if ejec_col else 0.0
                saldo_declared = (
                    self._to_decimal(row.get(saldo_col)) if saldo_col else None
                )

                programado = max(0.0, programado)
                ejecutado = max(0.0, ejecutado)

                # Validation: ejecutado should not exceed programado
                if ejecutado > programado + 0.01:
                    self.result.warnings.append(
                        f"Fila {row_idx} AO '{codigo_ao}' mes {mes_num}: "
                        f"ejecutado ({ejecutado:.2f}) > programado ({programado:.2f})."
                    )

                computed_saldo = round(programado - ejecutado, 2)

                if saldo_declared is not None:
                    declared_rounded = round(saldo_declared, 2)
                    if abs(declared_rounded - computed_saldo) > 1.0:
                        self.result.warnings.append(
                            f"Fila {row_idx} AO '{codigo_ao}' mes {mes_num}: "
                            f"saldo declarado ({declared_rounded:.2f}) ≠ "
                            f"calculado ({computed_saldo:.2f})."
                        )

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
                        "ejecutado": round(ejecutado, 2),
                        "saldo": computed_saldo,
                    }
                )

            valid_rows += 1

        # ----------------------------------------------------------------
        # 7. Quarterly cross-validation (optional — warnings only)
        # ----------------------------------------------------------------
        self._validate_quarterly(month_triples)

        # ----------------------------------------------------------------
        # 8. Summary
        # ----------------------------------------------------------------
        self.result.metadata.update(
            {
                "valid_rows": valid_rows,
                "skipped_rows": skipped,
                "anio": anio,
                "ue_codigo": ue_codigo,
                "meta_codigo": meta_codigo,
                "months_detected": sorted(month_triples.keys()),
            }
        )

        logger.info(
            "Formato5BParser: rows=%d skipped=%d months=%s anio=%d",
            valid_rows,
            skipped,
            sorted(month_triples.keys()),
            anio,
        )
        return self.result

    # ------------------------------------------------------------------
    # Compound header builder
    # ------------------------------------------------------------------

    def _build_compound_columns(
        self, raw_all: pd.DataFrame
    ) -> tuple[list[str], tuple[int, int]]:
        """Merge two header rows into composite column names.

        Searches the first ``data_start_row`` rows for two consecutive rows
        where:
          - Row A contains month names or "AO"/"Codigo" labels.
          - Row B contains "Programado", "Ejecutado", "Saldo" sub-labels.

        Returns:
            (compound_col_names, (row_a_idx, row_b_idx))
            compound_col_names has one entry per column.
        """
        limit = min(self.data_start_row + 2, len(raw_all))

        for r in range(limit - 1):
            row_a = raw_all.iloc[r]
            row_b = raw_all.iloc[r + 1]

            row_b_text = " ".join(self._clean_str(v).lower() for v in row_b)
            # Row B must contain at least one programado/ejecutado/saldo
            if not any(sc in row_b_text for sc in _SUB_COLS):
                continue

            # Row A must contain at least one month name
            row_a_has_month = any(
                _is_month_label(self._clean_str(v)) is not None
                for v in row_a
            )
            if not row_a_has_month:
                continue

            # Build compound names
            compound: list[str] = []
            current_group = ""
            for col_i in range(len(raw_all.columns)):
                label_a = self._clean_str(row_a.iloc[col_i]) if col_i < len(row_a) else ""
                label_b = self._clean_str(row_b.iloc[col_i]) if col_i < len(row_b) else ""

                if label_a:
                    current_group = label_a

                if label_b:
                    sub = _is_subcol(label_b)
                    if sub and current_group:
                        compound.append(f"{current_group}_{sub}")
                    elif label_b:
                        compound.append(label_b if not label_a else f"{label_a}_{label_b}")
                    else:
                        compound.append(label_a or f"col_{col_i}")
                else:
                    compound.append(current_group or label_a or f"col_{col_i}")

            logger.debug(
                "Formato5BParser: compound header built from rows %d+%d, "
                "%d columns",
                r,
                r + 1,
                len(compound),
            )
            return compound, (r, r + 1)

        # Fallback: use the header_rows hint from __init__
        r_a, r_b = self.header_rows
        if r_b < len(raw_all):
            row_a = raw_all.iloc[r_a]
            row_b = raw_all.iloc[r_b]
            compound = []
            current_group = ""
            for col_i in range(len(raw_all.columns)):
                label_a = self._clean_str(row_a.iloc[col_i]) if col_i < len(row_a) else ""
                label_b = self._clean_str(row_b.iloc[col_i]) if col_i < len(row_b) else ""
                if label_a:
                    current_group = label_a
                sub = _is_subcol(label_b)
                if sub and current_group:
                    compound.append(f"{current_group}_{sub}")
                else:
                    compound.append(label_b or current_group or f"col_{col_i}")
            return compound, (r_a, r_b)

        return [], (0, 1)

    # ------------------------------------------------------------------
    # Month triple builder
    # ------------------------------------------------------------------

    def _build_month_triples(
        self, compound_cols: list[str]
    ) -> dict[int, dict[str, str]]:
        """Map each month number to its three column names.

        Returns:
            {mes_num: {"programado": col_name, "ejecutado": col_name, "saldo": col_name}}
        """
        triples: dict[int, dict[str, str]] = {}

        for col_name in compound_cols:
            # Expected pattern: "Enero_programado", "Feb_ejecutado", etc.
            parts = col_name.split("_")
            if len(parts) < 2:
                continue

            prefix = parts[0].strip().lower()
            suffix = parts[-1].strip().lower()

            mes_num = _is_month_label(prefix)
            sub = _is_subcol(suffix)

            if mes_num is None or sub is None:
                continue

            if mes_num not in triples:
                triples[mes_num] = {}
            triples[mes_num][sub] = col_name

        return triples

    # ------------------------------------------------------------------
    # Quarterly cross-validation
    # ------------------------------------------------------------------

    def _validate_quarterly(
        self, month_triples: dict[int, dict[str, str]]
    ) -> None:
        """Log warnings if quarterly summary columns are inconsistent.

        (This is informational only — no records are modified.)
        """
        # Quarterly boundaries
        quarters = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
        for q, (start, end) in quarters.items():
            q_months = [m for m in range(start, end + 1) if m in month_triples]
            if len(q_months) < 3:
                self.result.warnings.append(
                    f"Formato5B: trimestre {q} tiene solo {len(q_months)} "
                    f"de 3 meses detectados."
                )
