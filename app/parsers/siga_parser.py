"""Parser for SIGA (Sistema Integrado de Gestión Administrativa) exports.

Expected Excel layout:
    Row 1-2: Title/context
    Row 3:   Column headers
    Row 4+:  Data rows

Expected columns (approximate):
    Nro. Requerimiento | Descripcion | Unidad Medida | Cantidad |
    Precio Unitario | Monto Total | Estado | Proveedor | Fecha

Since there is no dedicated DB table for SIGA data, records are emitted
as generic dicts and counted as processed in the audit log.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParseResult

logger = logging.getLogger(__name__)

_COL_ALIASES: dict[str, list[str]] = {
    "numero_requerimiento": [
        "nro", "numero", "número", "requerimiento", "nro. requerimiento",
        "nro requerimiento", "n° requerimiento",
    ],
    "descripcion": [
        "descripcion", "descripción", "detalle", "bien/servicio",
        "descripcion del bien", "item",
    ],
    "unidad_medida": ["unidad", "unidad medida", "u.m.", "um", "unid"],
    "cantidad": ["cantidad", "cant", "qty"],
    "precio_unitario": ["precio unitario", "p.u.", "precio", "costo unitario"],
    "monto_total": [
        "monto total", "total", "monto", "valor total", "importe",
    ],
    "estado": ["estado", "situacion", "situación", "status"],
    "proveedor": ["proveedor", "razón social", "razon social", "empresa"],
    "fecha": ["fecha", "date", "fecha requerimiento"],
}


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


class SigaParser(BaseParser):
    """Parse SIGA logistics system exports."""

    FORMAT_NAME = "SIGA"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 3,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        errors: list[str] = []
        # SIGA is very flexible — just check we have at least a description-like column
        has_desc = _match_column(df, _COL_ALIASES["descripcion"]) is not None
        has_monto = _match_column(df, _COL_ALIASES["monto_total"]) is not None
        if not has_desc and not has_monto:
            errors.append(
                "SIGA: no se encontró columna 'descripcion' ni 'monto_total'. "
                f"Columnas detectadas: {list(df.columns)}"
            )
        return errors

    def parse(self) -> ParseResult:
        self.result.format_name = self.FORMAT_NAME

        # 1. Context — load enough rows to find the header
        raw_head = self._load_raw_rows(sheet_name=self.sheet_name, nrows=max(self.data_start_row + 2, 8))

        # 2. Detect header
        header_row_idx = self._detect_header_row(raw_head)

        # 3. Load
        df = self._load_sheet(
            sheet_name=self.sheet_name,
            header=header_row_idx,
            dtype=str,
        )
        if df.empty:
            self.result.errors.append("SIGA: la hoja está vacía.")
            return self.result

        df.columns = [self._clean_str(c) for c in df.columns]

        # 4. Validate
        struct_errors = self.validate_structure(df)
        self.result.errors.extend(struct_errors)
        if struct_errors:
            return self.result

        # 5. Resolve columns
        col_nro = _match_column(df, _COL_ALIASES["numero_requerimiento"])
        col_desc = _match_column(df, _COL_ALIASES["descripcion"])
        col_um = _match_column(df, _COL_ALIASES["unidad_medida"])
        col_cant = _match_column(df, _COL_ALIASES["cantidad"])
        col_pu = _match_column(df, _COL_ALIASES["precio_unitario"])
        col_total = _match_column(df, _COL_ALIASES["monto_total"])
        col_estado = _match_column(df, _COL_ALIASES["estado"])
        col_prov = _match_column(df, _COL_ALIASES["proveedor"])
        col_fecha = _match_column(df, _COL_ALIASES["fecha"])

        # 6. Iterate
        valid_rows = 0
        skipped = 0
        rows_to_skip = max(0, self.data_start_row - header_row_idx - 1)

        for row_idx, row in df.iterrows():
            if int(row_idx) < rows_to_skip:
                continue
            if self._is_empty_row(row):
                continue

            desc = self._clean_str(row.get(col_desc, "")) if col_desc else ""
            if not desc:
                skipped += 1
                continue

            self.result.records.append({
                "_type": "siga_requerimiento",
                "numero_requerimiento": self._clean_str(row.get(col_nro, "")) if col_nro else "",
                "descripcion": desc,
                "unidad_medida": self._clean_str(row.get(col_um, "")) if col_um else "",
                "cantidad": self._to_decimal(row.get(col_cant)) if col_cant else 0,
                "precio_unitario": self._to_decimal(row.get(col_pu)) if col_pu else 0,
                "monto_total": self._to_decimal(row.get(col_total)) if col_total else 0,
                "estado": self._clean_str(row.get(col_estado, "")) if col_estado else "",
                "proveedor": self._clean_str(row.get(col_prov, "")) if col_prov else "",
                "fecha": self._clean_str(row.get(col_fecha, "")) if col_fecha else "",
            })
            valid_rows += 1

        self.result.metadata.update({
            "valid_rows": valid_rows,
            "skipped_rows": skipped,
        })

        logger.info("SigaParser: rows=%d skipped=%d", valid_rows, skipped)
        return self.result

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        """Find the header row by picking the one with the most keyword matches."""
        keywords = ("requerimiento", "descripcion", "monto", "cantidad", "estado", "proveedor")
        best_row = max(0, self.data_start_row - 1)
        best_score = 0
        for r in range(min(8, len(raw_head))):
            row_text = " ".join(self._clean_str(v).lower() for v in raw_head.iloc[r])
            score = sum(1 for kw in keywords if kw in row_text)
            if score > best_score:
                best_score = score
                best_row = r
        return best_row
