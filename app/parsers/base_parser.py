"""Abstract base class for all INEI Excel format parsers.

Provides shared infrastructure for loading workbooks, extracting header
context metadata, and normalising cell values before format-specific
subclasses do their domain logic.
"""

from __future__ import annotations

import io
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class ParseResult:
    """Container returned by every parser after processing a workbook.

    Attributes:
        records: List of dicts ready for bulk-insert / Pydantic validation.
            Each dict key matches a model field name.
        errors: Fatal row-level or structural problems (row was skipped).
        warnings: Non-fatal oddities (row was kept but may need review).
        metadata: Header context extracted from the file (UE, meta, year …).
        format_name: Detected or assumed format identifier string.
    """

    records: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    format_name: str = "DESCONOCIDO"

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @property
    def ok(self) -> bool:
        """True when no fatal errors were collected."""
        return len(self.errors) == 0

    @property
    def record_count(self) -> int:
        """Number of successfully parsed data records."""
        return len(self.records)

    def summary(self) -> str:
        """One-line human-readable summary of the parse run."""
        status = "OK" if self.ok else "ERROR"
        return (
            f"[{status}] format={self.format_name} "
            f"records={self.record_count} "
            f"errors={len(self.errors)} "
            f"warnings={len(self.warnings)}"
        )


# ---------------------------------------------------------------------------
# Base parser
# ---------------------------------------------------------------------------


class BaseParser(ABC):
    """Abstract base for all Excel format parsers.

    Subclasses must implement:
        * ``validate_structure(df)`` — check expected columns / shape.
        * ``parse()``               — extract domain records.

    The constructor accepts a file path string, raw bytes, or an open
    binary-mode file object so it works both from the filesystem and from
    FastAPI ``UploadFile.read()``.

    Attributes:
        file_source: The original argument passed to the constructor.
        workbook_bytes: Raw bytes of the workbook, kept for re-parsing.
        result: Accumulated ``ParseResult`` (populated during ``parse()``).
    """

    # Name to assign in ``ParseResult.format_name``; override in subclasses.
    FORMAT_NAME: str = "DESCONOCIDO"

    def __init__(self, file_path_or_bytes: str | bytes | BinaryIO) -> None:
        self.file_source = file_path_or_bytes
        self.workbook_bytes: bytes = self._read_source(file_path_or_bytes)
        self.result: ParseResult = ParseResult(format_name=self.FORMAT_NAME)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _read_source(source: str | bytes | BinaryIO) -> bytes:
        """Normalise any input type to raw bytes."""
        if isinstance(source, bytes):
            return source
        if isinstance(source, str):
            return Path(source).read_bytes()
        # File-like object (e.g. SpooledTemporaryFile from FastAPI)
        pos = getattr(source, "tell", lambda: None)()
        data = source.read()
        if pos is not None:
            try:
                source.seek(pos)
            except Exception:
                pass
        return data if isinstance(data, bytes) else data.encode()

    def _open_excel(self) -> io.BytesIO:
        """Return a BytesIO handle positioned at byte 0."""
        return io.BytesIO(self.workbook_bytes)

    # ------------------------------------------------------------------
    # Sheet loading
    # ------------------------------------------------------------------

    def _load_sheet(
        self,
        sheet_name: str | int = 0,
        header: int | list[int] | None = None,
        skiprows: int | list[int] | None = None,
        nrows: int | None = None,
        dtype: type | dict | None = str,
    ) -> pd.DataFrame:
        """Load a worksheet into a DataFrame using the openpyxl engine.

        By default every cell is read as a string so that formulae and
        mixed-type columns do not cause silent data loss.  Callers that need
        numeric columns should cast after loading.

        Args:
            sheet_name: Sheet index (0-based) or exact sheet name.
            header:  Row index (0-based) to use as column names, or None for
                     no header (columns become 0, 1, 2 …).
            skiprows: Rows to skip before the header/data area.
            nrows:   Maximum number of data rows to read.
            dtype:   dtype override passed to ``pd.read_excel``.

        Returns:
            DataFrame with forward-filled merged-cell values.
        """
        try:
            df = pd.read_excel(
                self._open_excel(),
                sheet_name=sheet_name,
                header=header,
                skiprows=skiprows,
                nrows=nrows,
                dtype=dtype,
                engine="openpyxl",
            )
        except Exception as exc:
            msg = f"Failed to load sheet '{sheet_name}': {exc}"
            logger.error(msg)
            self.result.errors.append(msg)
            return pd.DataFrame()

        # Forward-fill merged-cell artefacts in the first few columns
        df = self._forward_fill_merged(df)
        return df

    def _load_raw_rows(
        self,
        sheet_name: str | int = 0,
        nrows: int | None = None,
    ) -> pd.DataFrame:
        """Load every row without a header, dtype=str, for context extraction.

        Args:
            sheet_name: Sheet index or name.
            nrows: Row count ceiling (useful for reading just the header area).

        Returns:
            DataFrame with integer column positions.
        """
        return self._load_sheet(
            sheet_name=sheet_name,
            header=None,
            nrows=nrows,
            dtype=str,
        )

    # ------------------------------------------------------------------
    # Merged-cell handling
    # ------------------------------------------------------------------

    @staticmethod
    def _forward_fill_merged(df: pd.DataFrame) -> pd.DataFrame:
        """Replace NaN artefacts left by merged cells with the last valid value.

        Only applied to the leftmost columns (up to 4) that are likely to
        carry hierarchical labels from merged header cells.
        """
        cols_to_ffill = df.columns[:4].tolist()
        df[cols_to_ffill] = df[cols_to_ffill].ffill()
        return df

    # ------------------------------------------------------------------
    # Context / metadata extraction
    # ------------------------------------------------------------------

    def _extract_context(
        self,
        raw_df: pd.DataFrame,
        context_rows: dict[str, tuple[int, int]],
    ) -> dict[str, str]:
        """Extract scalar metadata values from header rows.

        Many INEI Excel files embed the executing unit name, meta code, and
        fiscal year in fixed cells above the data table.  This method reads
        those cells by (row, col) position.

        Args:
            raw_df: Full raw DataFrame (no header, all strings).
            context_rows: Mapping of field_name → (row_index, col_index).
                Both indices are **0-based**.

        Returns:
            Dict of field_name → stripped string value (empty string if cell
            is missing or blank).

        Example::

            ctx = self._extract_context(raw, {
                "ue_nombre": (2, 3),
                "anio":      (3, 3),
                "meta":      (4, 3),
            })
        """
        context: dict[str, str] = {}
        for field_name, (row_idx, col_idx) in context_rows.items():
            try:
                raw_val = raw_df.iloc[row_idx, col_idx]
                context[field_name] = self._clean_str(raw_val)
            except (IndexError, KeyError):
                context[field_name] = ""
                logger.debug(
                    "Context field '%s' not found at (%d, %d)",
                    field_name,
                    row_idx,
                    col_idx,
                )
        return context

    def _scan_for_value(
        self,
        raw_df: pd.DataFrame,
        label: str,
        search_rows: int = 15,
        col_offset: int = 1,
    ) -> str:
        """Scan header rows for a label and return the adjacent cell value.

        Useful when cell positions vary between file versions.

        Args:
            raw_df: Raw DataFrame (no header).
            label: Text to search for (case-insensitive, partial match).
            search_rows: How many rows from the top to scan.
            col_offset: Column offset from the found label cell to the value.

        Returns:
            Stripped value string, or empty string if not found.
        """
        label_lower = label.lower()
        for r in range(min(search_rows, len(raw_df))):
            for c in range(len(raw_df.columns)):
                cell = self._clean_str(raw_df.iloc[r, c])
                if label_lower in cell.lower():
                    try:
                        val = raw_df.iloc[r, c + col_offset]
                        return self._clean_str(val)
                    except (IndexError, KeyError):
                        pass
        return ""

    # ------------------------------------------------------------------
    # Value normalisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_str(value: Any) -> str:
        """Return a stripped string, converting NaN/None to empty string."""
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _to_decimal(value: Any, default: float = 0.0) -> float:
        """Parse a cell value to float, stripping formatting artefacts.

        Handles thousands separators (commas), leading/trailing spaces,
        currency symbols, and pure NaN.
        """
        if value is None:
            return default
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return default
            return float(value)
        cleaned = re.sub(r"[,\s]", "", str(value).strip().lstrip("S/.$ "))
        if not cleaned or cleaned in ("-", "—"):
            return default
        try:
            return float(cleaned)
        except ValueError:
            return default

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        """Parse a cell value to int."""
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _normalize_clasificador(code: Any) -> str:
        """Normalize a classifier code to "X.X.X.X.X.X" format.

        Strips spaces and ensures dots are used as separators.
        """
        raw = BaseParser._clean_str(code)
        # Remove all whitespace
        raw = re.sub(r"\s+", "", raw)
        return raw

    # ------------------------------------------------------------------
    # Row-filtering helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_empty_row(row: pd.Series) -> bool:
        """True when every non-NaN cell in the row is an empty string."""
        for val in row:
            clean = BaseParser._clean_str(val)
            if clean:
                return False
        return True

    @staticmethod
    def _is_header_row(row: pd.Series, keywords: list[str]) -> bool:
        """True when the row looks like a repeated column header.

        Checks whether any cell contains one of the given keywords
        (case-insensitive substring match).
        """
        for val in row:
            text = BaseParser._clean_str(val).lower()
            for kw in keywords:
                if kw.lower() in text:
                    return True
        return False

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that the DataFrame has the required columns / shape.

        Args:
            df: The main data DataFrame (already loaded by the subclass).

        Returns:
            List of error messages.  Empty list means structure is valid.
        """

    @abstractmethod
    def parse(self) -> ParseResult:
        """Execute the full parsing pipeline and return a ``ParseResult``.

        Implementations should:
        1. Load the relevant sheet(s) via ``_load_sheet`` / ``_load_raw_rows``.
        2. Call ``validate_structure`` and append errors to ``self.result``.
        3. Iterate data rows, calling ``_to_decimal`` / ``_clean_str`` etc.
        4. Append valid dicts to ``self.result.records``.
        5. Log skipped rows to ``self.result.warnings``.
        6. Return ``self.result``.
        """
