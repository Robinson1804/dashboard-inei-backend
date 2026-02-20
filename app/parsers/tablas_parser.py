"""Parser for the Tablas sheet — 569 SIAF expenditure classifier codes.

The Tablas sheet is a master reference table that populates the
``ClasificadorGasto`` model.  Expected column layout:

    Codigo  |  Descripcion  |  Tipo Generico

Where:
    Codigo       — hierarchical code, e.g. "2.3.1.5.1.2"
    Descripcion  — full text name of the expenditure line
    Tipo Generico — top-level group: "2.1", "2.3", "2.5", or "2.6"

The parser normalises codes to a canonical dot-separated format and
validates them against the expected pattern before emitting records.
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from .base_parser import BaseParser, ParseResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Canonical classifier pattern — e.g. "2.3.1.5.1.2"
# Levels 1–6 with one or more digits each
_CLASIFICADOR_RE = re.compile(r"^\d+(\.\d+){1,5}$")

# Valid top-level tipo_generico values
_VALID_TIPO_GENERICO = {"2.1", "2.3", "2.5", "2.6"}

# Column name aliases (case-insensitive substring matching)
_COL_ALIASES: dict[str, list[str]] = {
    "codigo": [
        "codigo", "código", "cod.", "clasificador", "cod. gasto",
        "codigo de gasto",
    ],
    "descripcion": [
        "descripcion", "descripción", "nombre", "denominacion",
        "descripcion del clasificador",
    ],
    "tipo_generico": [
        "tipo generico", "tipo genérico", "tipo", "generico", "genérico",
        "grupo generico", "grupo genérico",
    ],
}


def _match_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Return the DataFrame column name that best matches one of the aliases."""
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for alias in aliases:
        alias_l = alias.lower().strip()
        if alias_l in cols_lower:
            return cols_lower[alias_l]
    # Substring fallback
    for alias in aliases:
        alias_l = alias.lower().strip()
        for col_lower, col_orig in cols_lower.items():
            if alias_l in col_lower:
                return col_orig
    return None


class TablasParser(BaseParser):
    """Parse the Tablas / ClasificadorGasto master reference sheet.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index (0-based) or sheet name string.
            The parser will try to auto-detect the correct sheet if
            ``sheet_name`` is 0.
        header_row: 0-based row index for column headers.  Defaults to 0.
    """

    FORMAT_NAME = "TABLAS"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        header_row: int = 0,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.header_row = header_row

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that ``codigo`` and ``descripcion`` columns are present."""
        errors: list[str] = []
        col_map = self._build_col_map(df)
        for required in ("codigo", "descripcion"):
            if col_map[required] is None:
                errors.append(
                    f"Tablas: columna requerida '{required}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Tablas parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Resolve sheet
        # ----------------------------------------------------------------
        sheet_to_use = self._resolve_sheet()

        # ----------------------------------------------------------------
        # 2. Detect header row
        # ----------------------------------------------------------------
        raw_head = self._load_raw_rows(sheet_name=sheet_to_use, nrows=10)
        header_row_idx = self._detect_header_row(raw_head)

        # ----------------------------------------------------------------
        # 3. Load main DataFrame
        # ----------------------------------------------------------------
        df = self._load_sheet(
            sheet_name=sheet_to_use,
            header=header_row_idx,
            dtype=str,
        )
        if df.empty:
            self.result.errors.append("Tablas: la hoja está vacía.")
            return self.result

        # Normalise column names
        df.columns = [self._clean_str(c) for c in df.columns]

        # ----------------------------------------------------------------
        # 4. Validate structure
        # ----------------------------------------------------------------
        struct_errors = self.validate_structure(df)
        self.result.errors.extend(struct_errors)
        if struct_errors:
            return self.result

        col_map = self._build_col_map(df)
        col_codigo = col_map["codigo"]
        col_desc = col_map["descripcion"]
        col_tipo = col_map["tipo_generico"]

        # ----------------------------------------------------------------
        # 5. Iterate rows
        # ----------------------------------------------------------------
        seen_codes: set[str] = set()
        skipped = 0

        for row_idx, row in df.iterrows():
            if self._is_empty_row(row):
                continue

            raw_code = self._clean_str(row[col_codigo])
            codigo = self._normalize_clasificador(raw_code)

            # Skip section header rows (e.g. "GRUPO GENERICO: 2.3")
            if not codigo or not _CLASIFICADOR_RE.match(codigo):
                # It might be a tipo_generico section header — try to extract it
                if _is_tipo_header(raw_code):
                    continue
                self.result.warnings.append(
                    f"Fila {row_idx}: código clasificador inválido "
                    f"('{raw_code}') — fila omitida."
                )
                skipped += 1
                continue

            descripcion = self._clean_str(row[col_desc]) if col_desc else ""
            if not descripcion:
                self.result.warnings.append(
                    f"Fila {row_idx}: descripción vacía para código "
                    f"'{codigo}' — fila omitida."
                )
                skipped += 1
                continue

            # Determine tipo_generico from explicit column or infer from code
            if col_tipo and col_tipo in df.columns:
                tipo_raw = self._clean_str(row[col_tipo])
                tipo_generico = _normalise_tipo(tipo_raw) or _infer_tipo(codigo)
            else:
                tipo_generico = _infer_tipo(codigo)

            # Duplicate check
            if codigo in seen_codes:
                self.result.warnings.append(
                    f"Fila {row_idx}: código duplicado '{codigo}' — segunda "
                    "ocurrencia omitida."
                )
                continue

            seen_codes.add(codigo)
            self.result.records.append(
                {
                    "_type": "clasificador_gasto",
                    "codigo": codigo,
                    "descripcion": descripcion,
                    "tipo_generico": tipo_generico,
                }
            )

        # ----------------------------------------------------------------
        # 6. Summary metadata
        # ----------------------------------------------------------------
        self.result.metadata.update(
            {
                "total_clasificadores": len(seen_codes),
                "skipped_rows": skipped,
                "tipos_genericos": sorted(
                    {r["tipo_generico"] for r in self.result.records if r.get("tipo_generico")}
                ),
            }
        )

        logger.info(
            "TablasParser: clasificadores=%d skipped=%d",
            len(seen_codes),
            skipped,
        )
        return self.result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_col_map(self, df: pd.DataFrame) -> dict[str, str | None]:
        return {field: _match_column(df, aliases) for field, aliases in _COL_ALIASES.items()}

    def _resolve_sheet(self) -> str | int:
        """Find the 'Tablas' sheet; fall back to index 0."""
        if isinstance(self.sheet_name, str):
            return self.sheet_name
        try:
            xl = pd.ExcelFile(self._open_excel(), engine="openpyxl")
            for name in xl.sheet_names:
                if "tabla" in name.lower():
                    logger.debug("TablasParser: using sheet '%s'", name)
                    return name
            return xl.sheet_names[0]
        except Exception:
            return 0

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        """Return the 0-based row index that contains the column headers."""
        for r in range(min(8, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if any(kw in row_text for kw in ("codigo", "descripcion", "clasificador")):
                return r
        return 0


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _is_tipo_header(raw: str) -> bool:
    """True if the cell text looks like a section header, not a code."""
    text = raw.strip().lower()
    return any(kw in text for kw in ("grupo", "generico", "genérico", "tipo"))


def _normalise_tipo(raw: str) -> str:
    """Extract a canonical tipo_generico string like '2.3'."""
    match = re.search(r"\b(2\.[1356])\b", raw)
    if match:
        val = match.group(1)
        # Map 2.5 and 2.6 to valid values; others stay as-is
        return val if val in _VALID_TIPO_GENERICO else ""
    return ""


def _infer_tipo(codigo: str) -> str:
    """Infer tipo_generico from the leading segment of a classifier code."""
    parts = codigo.split(".")
    if len(parts) >= 2:
        candidate = f"{parts[0]}.{parts[1]}"
        if candidate in _VALID_TIPO_GENERICO:
            return candidate
    return ""
