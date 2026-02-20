"""Parser for SIAF (Sistema Integrado de Administración Financiera) exports.

Expected Excel layout:
    Row 1-3: Title/context (year, entity, etc.)
    Row 4:   Column headers
    Row 5+:  Data rows

Expected columns (approximate — tolerant matching):
    Año | Mes | Sec.Func | Clasificador | Descripcion |
    PIA | PIM | Certificado | Compromiso Anual | Devengado | Girado

Records emitted:
    - ``_type: "programacion_presupuestal"`` with budget execution fields.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParseResult

logger = logging.getLogger(__name__)

_CLASIFICADOR_RE = re.compile(r"^\d+(\.\d+){1,5}$")

_COL_ALIASES: dict[str, list[str]] = {
    "anio": ["año", "anio", "ano", "ejercicio", "year"],
    "mes": ["mes", "month", "periodo"],
    "sec_funcional": [
        "sec.func", "sec func", "sec_func", "secuencia funcional",
        "secuencia", "sec. funcional",
    ],
    "clasificador": [
        "clasificador", "código", "codigo", "cod. gasto", "clasificador de gasto",
    ],
    "descripcion": [
        "descripcion", "descripción", "nombre", "denominacion",
    ],
    "pia": ["pia", "presupuesto inicial"],
    "pim": ["pim", "presupuesto modificado", "presupuesto institucional modificado"],
    "certificado": ["certificado", "certificacion", "certificación", "ccp"],
    "compromiso_anual": ["compromiso", "compromiso anual", "comp. anual"],
    "devengado": ["devengado", "deveng.", "deveng"],
    "girado": ["girado", "giro", "pagado"],
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


class SiafParser(BaseParser):
    """Parse SIAF financial system exports."""

    FORMAT_NAME = "SIAF"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 4,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        errors: list[str] = []
        if _match_column(df, _COL_ALIASES["clasificador"]) is None:
            errors.append(
                "SIAF: columna 'clasificador' no encontrada. "
                f"Columnas detectadas: {list(df.columns)}"
            )
        if _match_column(df, _COL_ALIASES["devengado"]) is None:
            errors.append(
                "SIAF: columna 'devengado' no encontrada. "
                f"Columnas detectadas: {list(df.columns)}"
            )
        return errors

    def parse(self) -> ParseResult:
        self.result.format_name = self.FORMAT_NAME

        # 1. Context extraction
        raw_head = self._load_raw_rows(sheet_name=self.sheet_name, nrows=self.data_start_row)
        context: dict[str, str] = {}
        context["anio"] = (
            self._scan_for_value(raw_head, "año")
            or self._scan_for_value(raw_head, "ejercicio")
            or self._extract_year_from_cells(raw_head)
        )
        context["ue_nombre"] = self._scan_for_value(raw_head, "entidad") or self._scan_for_value(raw_head, "unidad")
        self.result.metadata.update(context)

        # 2. Detect header row
        header_row_idx = self._detect_header_row(raw_head)

        # 3. Load DataFrame
        df = self._load_sheet(
            sheet_name=self.sheet_name,
            header=header_row_idx,
            dtype=str,
        )
        if df.empty:
            self.result.errors.append("SIAF: la hoja está vacía.")
            return self.result

        df.columns = [self._clean_str(c) for c in df.columns]

        # 4. Validate structure
        struct_errors = self.validate_structure(df)
        self.result.errors.extend(struct_errors)
        if struct_errors:
            return self.result

        # 5. Resolve columns
        col_anio = _match_column(df, _COL_ALIASES["anio"])
        col_clas = _match_column(df, _COL_ALIASES["clasificador"])
        col_pia = _match_column(df, _COL_ALIASES["pia"])
        col_pim = _match_column(df, _COL_ALIASES["pim"])
        col_cert = _match_column(df, _COL_ALIASES["certificado"])
        col_comp = _match_column(df, _COL_ALIASES["compromiso_anual"])
        col_dev = _match_column(df, _COL_ALIASES["devengado"])
        col_gir = _match_column(df, _COL_ALIASES["girado"])

        try:
            default_anio = int(float(context.get("anio", "0") or "0"))
        except (ValueError, TypeError):
            default_anio = 2026

        # 6. Iterate rows
        valid_rows = 0
        skipped = 0
        rows_to_skip = max(0, self.data_start_row - header_row_idx - 1)

        for row_idx, row in df.iterrows():
            if int(row_idx) < rows_to_skip:
                continue
            if self._is_empty_row(row):
                continue
            if self._is_header_row(row, ["clasificador", "devengado", "pia", "pim"]):
                continue

            raw_clas = self._clean_str(row.get(col_clas, "")) if col_clas else ""
            clasificador = self._normalize_clasificador(raw_clas)

            if not clasificador or not _CLASIFICADOR_RE.match(clasificador):
                if clasificador:
                    self.result.warnings.append(
                        f"Fila {row_idx}: clasificador inválido ('{clasificador}') — omitida."
                    )
                skipped += 1
                continue

            anio = self._to_int(row.get(col_anio)) if col_anio else default_anio
            pia = self._to_decimal(row.get(col_pia)) if col_pia else 0.0
            pim = self._to_decimal(row.get(col_pim)) if col_pim else 0.0
            certificado = self._to_decimal(row.get(col_cert)) if col_cert else 0.0
            compromiso = self._to_decimal(row.get(col_comp)) if col_comp else 0.0
            devengado = self._to_decimal(row.get(col_dev)) if col_dev else 0.0
            girado = self._to_decimal(row.get(col_gir)) if col_gir else 0.0

            self.result.records.append({
                "_type": "programacion_presupuestal",
                "anio": anio or default_anio,
                "clasificador_codigo": clasificador,
                "pia": round(pia, 2),
                "pim": round(pim, 2),
                "certificado": round(certificado, 2),
                "compromiso_anual": round(compromiso, 2),
                "devengado": round(devengado, 2),
                "girado": round(girado, 2),
                "saldo": round(pim - devengado, 2),
            })
            valid_rows += 1

        self.result.metadata.update({
            "valid_rows": valid_rows,
            "skipped_rows": skipped,
            "anio": default_anio,
        })

        logger.info("SiafParser: rows=%d skipped=%d", valid_rows, skipped)
        return self.result

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        for r in range(min(10, len(raw_head))):
            row_text = " ".join(self._clean_str(v).lower() for v in raw_head.iloc[r])
            if any(kw in row_text for kw in ("clasificador", "devengado", "girado", "certificado")):
                return r
        return max(0, self.data_start_row - 1)

    @staticmethod
    def _extract_year_from_cells(raw_df: pd.DataFrame) -> str:
        """Extract a 4-digit year from any cell text like 'Ejercicio: 2026'."""
        for r in range(min(10, len(raw_df))):
            for c in range(len(raw_df.columns)):
                cell = str(raw_df.iloc[r, c] or "")
                match = re.search(r"(20[2-3]\d)", cell)
                if match:
                    return match.group(1)
        return ""
