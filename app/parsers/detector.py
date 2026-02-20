"""Auto-detect the INEI Excel format from sheet names and header fingerprints.

Detection strategy (in priority order):
1. Sheet name exact / partial match against known patterns.
2. Column header keywords found in the first 15 rows of sheet 0.
3. Column count heuristics for formats with very distinctive widths.

Returns one of the ``FORMAT_*`` constants defined in ``KNOWN_FORMATS``.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import BinaryIO

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public format name constants
# ---------------------------------------------------------------------------

FORMAT_CUADRO_AO_META = "CUADRO_AO_META"
FORMAT_TABLAS = "TABLAS"
FORMAT_1 = "FORMATO_1"
FORMAT_2 = "FORMATO_2"
FORMAT_3 = "FORMATO_3"
FORMAT_04 = "FORMATO_04"
FORMAT_5A = "FORMATO_5A"
FORMAT_5B = "FORMATO_5B"
FORMAT_5_RESUMEN = "FORMATO_5_RESUMEN"
FORMAT_ANEXO_01 = "ANEXO_01"
FORMAT_SIAF = "SIAF"
FORMAT_SIGA = "SIGA"
FORMAT_UNKNOWN = "DESCONOCIDO"

KNOWN_FORMATS: tuple[str, ...] = (
    FORMAT_CUADRO_AO_META,
    FORMAT_TABLAS,
    FORMAT_1,
    FORMAT_2,
    FORMAT_3,
    FORMAT_04,
    FORMAT_5A,
    FORMAT_5B,
    FORMAT_5_RESUMEN,
    FORMAT_ANEXO_01,
    FORMAT_SIAF,
    FORMAT_SIGA,
    FORMAT_UNKNOWN,
)

# ---------------------------------------------------------------------------
# Internal detection rules
# ---------------------------------------------------------------------------

# (sheet_name_fragment_lower, format_constant)  — checked case-insensitively
_SHEET_NAME_RULES: list[tuple[str, str]] = [
    ("cuadro ao-meta", FORMAT_CUADRO_AO_META),
    ("cuadro ao meta", FORMAT_CUADRO_AO_META),
    ("ao-meta", FORMAT_CUADRO_AO_META),
    ("tablas", FORMAT_TABLAS),
    ("formato 1", FORMAT_1),
    ("formato1", FORMAT_1),
    ("formato 2", FORMAT_2),
    ("formato2", FORMAT_2),
    ("formato 3", FORMAT_3),
    ("formato3", FORMAT_3),
    ("formato 04", FORMAT_04),
    ("formato04", FORMAT_04),
    ("formato 4", FORMAT_04),
    ("formato4", FORMAT_04),
    ("formato 5.a", FORMAT_5A),
    ("formato5a", FORMAT_5A),
    ("formato 5a", FORMAT_5A),
    ("f5a", FORMAT_5A),
    ("formato 5.b", FORMAT_5B),
    ("formato5b", FORMAT_5B),
    ("formato 5b", FORMAT_5B),
    ("f5b", FORMAT_5B),
    ("formato 5 resumen", FORMAT_5_RESUMEN),
    ("5 resumen", FORMAT_5_RESUMEN),
    ("resumen 5", FORMAT_5_RESUMEN),
    ("5-resumen", FORMAT_5_RESUMEN),
    ("5resumen", FORMAT_5_RESUMEN),
    ("5_resumen", FORMAT_5_RESUMEN),
    ("anexo 01", FORMAT_ANEXO_01),
    ("anexo01", FORMAT_ANEXO_01),
    ("anexo_01", FORMAT_ANEXO_01),
    ("siaf", FORMAT_SIAF),
    ("siga", FORMAT_SIGA),
]

# Keyword sets that must ALL be present in the first ``_HEADER_SCAN_ROWS``
# rows to match the format.  Each entry: (frozenset_of_keywords, format_constant).
# Keywords are matched case-insensitively as substrings.
_HEADER_KEYWORD_RULES: list[tuple[frozenset[str], str]] = [
    # CUADRO AO-META: contains CEPLAN + AEI
    (frozenset({"ceplan", "aei", "oei"}), FORMAT_CUADRO_AO_META),
    # TABLAS: classifier table
    (frozenset({"clasificador", "tipo generico"}), FORMAT_TABLAS),
    (frozenset({"clasificador", "tipo genérico"}), FORMAT_TABLAS),
    # FORMATO 1: PIA + PIM + months
    (frozenset({"pia", "pim", "clasificador"}), FORMAT_1),
    # FORMATO 04: habilitadora + habilitada
    (frozenset({"habilitadora", "habilitada", "clasificador"}), FORMAT_04),
    (frozenset({"asignado", "habilitadora", "habilitada"}), FORMAT_04),
    # FORMATO 5.B: programado + ejecutado + saldo (triple headers)
    (frozenset({"programado", "ejecutado", "saldo"}), FORMAT_5B),
    # FORMATO 5.A: programado only (no ejecutado column)
    (frozenset({"programado", "codigo ao"}), FORMAT_5A),
    (frozenset({"programado", "código ao"}), FORMAT_5A),
    # FORMATO 5 RESUMEN: AO summary with semaforo — must come BEFORE SIAF
    (frozenset({"codigo ao", "devengado", "semaforo"}), FORMAT_5_RESUMEN),
    (frozenset({"codigo ao", "devengado", "semáforo"}), FORMAT_5_RESUMEN),
    (frozenset({"codigo ao", "devengado", "% avance pim"}), FORMAT_5_RESUMEN),
    (frozenset({"código ao", "devengado", "semaforo"}), FORMAT_5_RESUMEN),
    # FORMATO 2: task-level programming with clasificador
    (frozenset({"cod tarea", "clasificador", "pim"}), FORMAT_2),
    (frozenset({"tarea", "clasificador", "cod ao"}), FORMAT_2),
    # FORMATO 3: tasks with justification
    (frozenset({"justificacion", "clasificador"}), FORMAT_3),
    (frozenset({"justificación", "clasificador"}), FORMAT_3),
    # ANEXO 01
    (frozenset({"dni", "remuneracion"}), FORMAT_ANEXO_01),
    (frozenset({"dni", "remuneración"}), FORMAT_ANEXO_01),
    (frozenset({"anexo", "certificacion"}), FORMAT_ANEXO_01),
    (frozenset({"anexo", "certificación"}), FORMAT_ANEXO_01),
    # SIAF export heuristic
    (frozenset({"devengado", "girado", "compromiso"}), FORMAT_SIAF),
    # SIGA export heuristic
    (frozenset({"siga", "requerimiento"}), FORMAT_SIGA),
]

# Distinctive column-count ranges: (min_cols, max_cols) → format
_COLUMN_COUNT_RULES: list[tuple[int, int, str]] = [
    (40, 50, FORMAT_5B),   # 45-column triple structure
    (20, 26, FORMAT_5A),   # 22-column programado-only
    (20, 26, FORMAT_1),    # 23-column PIA/PIM + 12 months + total
    (4, 8, FORMAT_04),     # 6 columns
]

_HEADER_SCAN_ROWS: int = 15


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_bytes(source: str | bytes | BinaryIO) -> bytes:
    if isinstance(source, bytes):
        return source
    if isinstance(source, str):
        return Path(source).read_bytes()
    pos = getattr(source, "tell", lambda: None)()
    data = source.read()
    if pos is not None:
        try:
            source.seek(pos)
        except Exception:
            pass
    return data if isinstance(data, bytes) else data.encode()


def _get_sheet_names(raw_bytes: bytes) -> list[str]:
    """Return all sheet names without loading full data."""
    try:
        xl = pd.ExcelFile(io.BytesIO(raw_bytes), engine="openpyxl")
        return xl.sheet_names
    except Exception as exc:
        logger.warning("Could not read sheet names: %s", exc)
        return []


def _load_header_area(raw_bytes: bytes, sheet: str | int = 0) -> pd.DataFrame:
    """Load first ``_HEADER_SCAN_ROWS`` rows as raw strings."""
    try:
        return pd.read_excel(
            io.BytesIO(raw_bytes),
            sheet_name=sheet,
            header=None,
            nrows=_HEADER_SCAN_ROWS,
            dtype=str,
            engine="openpyxl",
        )
    except Exception as exc:
        logger.warning("Could not load header area from sheet %s: %s", sheet, exc)
        return pd.DataFrame()


def _all_cell_text(df: pd.DataFrame) -> str:
    """Concatenate all cell values in the DataFrame into one lowercase string."""
    parts: list[str] = []
    for col in df.columns:
        for val in df[col]:
            if isinstance(val, str) and val.strip():
                parts.append(val.strip().lower())
    return " | ".join(parts)


def _count_data_columns(raw_bytes: bytes, sheet: str | int = 0) -> int:
    """Return the number of non-empty columns in row ``_HEADER_SCAN_ROWS - 1``."""
    try:
        df = pd.read_excel(
            io.BytesIO(raw_bytes),
            sheet_name=sheet,
            header=None,
            dtype=str,
            engine="openpyxl",
        )
        if df.empty:
            return 0
        # Use the row with the maximum non-null cell count as the header row
        non_null = df.notna().sum(axis=1)
        best_row = int(non_null.idxmax())
        return int(non_null.iloc[best_row])
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Main detection function
# ---------------------------------------------------------------------------


def detect_format(file_path_or_bytes: str | bytes | BinaryIO) -> str:
    """Determine which INEI Excel format the file conforms to.

    Detection is performed in three passes:
    1. Sheet name matching (fastest, most reliable).
    2. Header-area keyword matching on sheet 0.
    3. Column-count heuristics (fallback).

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.

    Returns:
        One of the ``FORMAT_*`` module-level constants, e.g. ``"FORMATO_1"``.
        Returns ``"DESCONOCIDO"`` when no rule matches.
    """
    try:
        raw = _read_bytes(file_path_or_bytes)
    except Exception as exc:
        logger.error("detect_format: cannot read source — %s", exc)
        return FORMAT_UNKNOWN

    sheet_names = _get_sheet_names(raw)
    logger.debug("detect_format: sheets found = %s", sheet_names)

    # ------------------------------------------------------------------
    # Pass 1: sheet name matching
    # ------------------------------------------------------------------
    for sheet_name in sheet_names:
        name_lower = sheet_name.lower().strip()
        for fragment, fmt in _SHEET_NAME_RULES:
            if fragment in name_lower:
                logger.info(
                    "detect_format: matched sheet name '%s' → %s", sheet_name, fmt
                )
                return fmt

    # ------------------------------------------------------------------
    # Pass 2: keyword matching in header area of sheet 0
    # ------------------------------------------------------------------
    header_df = _load_header_area(raw, sheet=0)
    all_text = _all_cell_text(header_df)
    logger.debug("detect_format: header text = %.200s …", all_text)

    for keywords, fmt in _HEADER_KEYWORD_RULES:
        if all(kw in all_text for kw in keywords):
            logger.info(
                "detect_format: matched keywords %s → %s", keywords, fmt
            )
            return fmt

    # ------------------------------------------------------------------
    # Pass 3: column count heuristics
    # ------------------------------------------------------------------
    col_count = _count_data_columns(raw, sheet=0)
    logger.debug("detect_format: data column count = %d", col_count)

    for min_c, max_c, fmt in _COLUMN_COUNT_RULES:
        if min_c <= col_count <= max_c:
            logger.info(
                "detect_format: matched col count %d in [%d, %d] → %s",
                col_count,
                min_c,
                max_c,
                fmt,
            )
            return fmt

    logger.warning("detect_format: no rule matched — returning DESCONOCIDO")
    return FORMAT_UNKNOWN
