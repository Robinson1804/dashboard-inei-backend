"""Parser for the CUADRO AO-META sheet.

This sheet defines the master hierarchy used throughout the system:

    UnidadEjecutora → MetaPresupuestal → ActividadOperativa

Column layout (approximate; column order may vary between years):
    Codigo UE | Nombre UE | Sigla | Codigo Meta | Sec. Funcional |
    Descripcion Meta | Codigo AO (CEPLAN) | Nombre AO | OEI | AEI

The parser emits three record-type dicts per distinct combination found:
    - type="unidad_ejecutora"    → maps to UnidadEjecutora model
    - type="meta_presupuestal"   → maps to MetaPresupuestal model
    - type="actividad_operativa" → maps to ActividadOperativa model

Merged cells are handled via forward-fill on the hierarchical columns.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from .base_parser import BaseParser, ParseResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column-name candidates (tolerant matching)
# ---------------------------------------------------------------------------

_COL_ALIASES: dict[str, list[str]] = {
    "codigo_ue": [
        "codigo ue", "código ue", "cod. ue", "cod ue",
        "codigo unidad", "código unidad",
    ],
    "nombre_ue": [
        "nombre ue", "nombre unidad ejecutora", "unidad ejecutora", "descripcion ue",
        "nombre de la unidad",
    ],
    "sigla": ["sigla", "siglas", "abreviatura"],
    "codigo_meta": [
        "codigo meta", "código meta", "cod. meta", "meta", "num. meta",
        "numero meta", "número meta",
    ],
    "sec_funcional": [
        "sec. funcional", "sec funcional", "secuencia funcional", "sec.func",
        "secuencia", "sec",
    ],
    "descripcion_meta": [
        "descripcion meta", "descripción meta", "nombre meta",
        "descripcion de la meta",
    ],
    "codigo_ceplan": [
        "codigo ao", "código ao", "codigo ceplan", "código ceplan",
        "cod. ao", "cod ao", "ceplan",
    ],
    "nombre_ao": [
        "nombre ao", "nombre actividad", "actividad operativa",
        "denominacion ao", "denominación ao",
    ],
    "oei": ["oei", "objetivo estrategico", "objetivo estratégico"],
    "aei": ["aei", "accion estrategica", "acción estratégica"],
}


def _match_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Find the first DataFrame column that fuzzy-matches any alias."""
    df_cols_lower = {c.lower().strip(): c for c in df.columns}
    for alias in aliases:
        alias_l = alias.lower().strip()
        # Exact match
        if alias_l in df_cols_lower:
            return df_cols_lower[alias_l]
        # Substring match
        for col_lower, col_orig in df_cols_lower.items():
            if alias_l in col_lower:
                return col_orig
    return None


def _build_col_map(df: pd.DataFrame) -> dict[str, str | None]:
    """Return a mapping of logical field name → actual DataFrame column name."""
    return {field: _match_column(df, aliases) for field, aliases in _COL_ALIASES.items()}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

_CEPLAN_PATTERN = re.compile(r"^[A-Z]{2,4}\d{8,}$", re.IGNORECASE)


def _valid_ceplan(code: str) -> bool:
    """Loose check: CEPLAN codes are alphanumeric, at least 10 chars."""
    if not code:
        return False
    # Accept codes like "AOI00000500001" or "AO00001234" — at least 8 chars
    cleaned = re.sub(r"\s+", "", code)
    return len(cleaned) >= 6 and re.search(r"\d", cleaned) is not None


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class CuadroAoMetaParser(BaseParser):
    """Parse the CUADRO AO-META master hierarchy sheet.

    Produces records of three entity types that can be upserted in order:
    1. ``"unidad_ejecutora"`` records (deduplicated by codigo_ue).
    2. ``"meta_presupuestal"`` records (deduplicated by codigo_ue + codigo_meta).
    3. ``"actividad_operativa"`` records (deduplicated by codigo_ceplan).

    Args:
        file_path_or_bytes: Path string, bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0 (first sheet).
        header_row: 0-based row index that contains the column headers.
            Defaults to 0.  Adjust if a title row precedes the headers.
        anio: Fiscal year to tag all records with.  Defaults to 0
            (caller should supply the correct year from the filename or UI).
    """

    FORMAT_NAME = "CUADRO_AO_META"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        header_row: int = 0,
        anio: int = 0,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.anio = anio

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Check that the minimum required columns are present."""
        errors: list[str] = []
        col_map = _build_col_map(df)
        required = ["codigo_ceplan", "nombre_ao"]
        for field in required:
            if col_map.get(field) is None:
                errors.append(
                    f"CUADRO AO-META: columna requerida no encontrada: '{field}'. "
                    f"Columnas presentes: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the parsing pipeline."""
        self.result.format_name = self.FORMAT_NAME

        # ----------------------------------------------------------------
        # 1. Try to auto-detect the correct sheet
        # ----------------------------------------------------------------
        sheet_to_use = self._resolve_sheet()

        # ----------------------------------------------------------------
        # 2. Load raw header area first to detect where data starts
        # ----------------------------------------------------------------
        raw_head = self._load_raw_rows(sheet_name=sheet_to_use, nrows=10)
        header_row_idx = self._detect_header_row(raw_head)

        # ----------------------------------------------------------------
        # 3. Load the main DataFrame
        # ----------------------------------------------------------------
        df = self._load_sheet(
            sheet_name=sheet_to_use,
            header=header_row_idx,
            dtype=str,
        )
        if df.empty:
            self.result.errors.append("CUADRO AO-META: la hoja está vacía.")
            return self.result

        # Normalize column names
        df.columns = [self._clean_str(c) for c in df.columns]

        # ----------------------------------------------------------------
        # 4. Validate structure
        # ----------------------------------------------------------------
        struct_errors = self.validate_structure(df)
        self.result.errors.extend(struct_errors)
        if struct_errors:
            return self.result

        col_map = _build_col_map(df)

        # ----------------------------------------------------------------
        # 5. Forward-fill hierarchical columns (merged cells)
        # ----------------------------------------------------------------
        hier_cols = [
            col_map["codigo_ue"],
            col_map["nombre_ue"],
            col_map["sigla"],
            col_map["codigo_meta"],
            col_map["sec_funcional"],
            col_map["descripcion_meta"],
        ]
        for col in hier_cols:
            if col and col in df.columns:
                df[col] = df[col].ffill()

        # ----------------------------------------------------------------
        # 6. Iterate rows and collect records
        # ----------------------------------------------------------------
        seen_ues: set[str] = set()
        seen_metas: set[tuple[str, str]] = set()
        seen_aos: set[str] = set()

        for row_idx, row in df.iterrows():
            # Skip blank rows and repeated header rows
            if self._is_empty_row(row):
                continue
            header_kws = ["codigo", "nombre", "sigla", "ceplan"]
            if self._is_header_row(row, header_kws):
                continue

            # ----------------------------------------------------------
            # Extract values
            # ----------------------------------------------------------
            def get(field: str) -> str:
                col = col_map.get(field)
                if col and col in df.columns:
                    return self._clean_str(row[col])
                return ""

            codigo_ue = get("codigo_ue")
            nombre_ue = get("nombre_ue")
            sigla = get("sigla")
            codigo_meta = get("codigo_meta")
            sec_funcional = get("sec_funcional")
            descripcion_meta = get("descripcion_meta")
            codigo_ceplan = self._clean_str(get("codigo_ceplan")).upper()
            nombre_ao = get("nombre_ao")
            oei = get("oei")
            aei = get("aei")

            # ----------------------------------------------------------
            # Validate AO code — required field
            # ----------------------------------------------------------
            if not _valid_ceplan(codigo_ceplan):
                self.result.warnings.append(
                    f"Fila {row_idx}: codigo_ceplan inválido o vacío "
                    f"('{codigo_ceplan}') — fila omitida."
                )
                continue

            if not nombre_ao:
                self.result.warnings.append(
                    f"Fila {row_idx}: nombre_ao vacío para codigo_ceplan "
                    f"'{codigo_ceplan}' — fila omitida."
                )
                continue

            # ----------------------------------------------------------
            # UnidadEjecutora record (deduplicated)
            # ----------------------------------------------------------
            if codigo_ue and codigo_ue not in seen_ues:
                seen_ues.add(codigo_ue)
                self.result.records.append(
                    {
                        "_type": "unidad_ejecutora",
                        "codigo": codigo_ue,
                        "nombre": nombre_ue,
                        "sigla": sigla,
                        "tipo": _infer_ue_tipo(sigla, nombre_ue),
                        "activo": True,
                    }
                )

            # ----------------------------------------------------------
            # MetaPresupuestal record (deduplicated per UE+meta)
            # ----------------------------------------------------------
            meta_key = (codigo_ue, codigo_meta)
            if codigo_meta and meta_key not in seen_metas:
                seen_metas.add(meta_key)
                self.result.records.append(
                    {
                        "_type": "meta_presupuestal",
                        "codigo": codigo_meta,
                        "descripcion": descripcion_meta,
                        "sec_funcional": sec_funcional,
                        "ue_codigo": codigo_ue,
                        "anio": self.anio,
                        "activo": True,
                    }
                )

            # ----------------------------------------------------------
            # ActividadOperativa record (deduplicated by CEPLAN code)
            # ----------------------------------------------------------
            if codigo_ceplan not in seen_aos:
                seen_aos.add(codigo_ceplan)
                self.result.records.append(
                    {
                        "_type": "actividad_operativa",
                        "codigo_ceplan": codigo_ceplan,
                        "nombre": nombre_ao,
                        "oei": oei,
                        "aei": aei,
                        "meta_codigo": codigo_meta,
                        "ue_codigo": codigo_ue,
                        "anio": self.anio,
                        "activo": True,
                    }
                )

        # ----------------------------------------------------------------
        # 7. Summary metadata
        # ----------------------------------------------------------------
        self.result.metadata.update(
            {
                "anio": self.anio,
                "total_ues": len(seen_ues),
                "total_metas": len(seen_metas),
                "total_aos": len(seen_aos),
            }
        )

        logger.info(
            "CuadroAoMetaParser: UEs=%d metas=%d AOs=%d warnings=%d",
            len(seen_ues),
            len(seen_metas),
            len(seen_aos),
            len(self.result.warnings),
        )
        return self.result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_sheet(self) -> str | int:
        """Find the best matching sheet for CUADRO AO-META."""
        if isinstance(self.sheet_name, str):
            return self.sheet_name  # caller was explicit

        try:
            xl = pd.ExcelFile(self._open_excel(), engine="openpyxl")
            for name in xl.sheet_names:
                lower = name.lower()
                if "ao" in lower and "meta" in lower:
                    logger.debug("CuadroAoMetaParser: using sheet '%s'", name)
                    return name
                if "cuadro" in lower:
                    return name
            return xl.sheet_names[0]
        except Exception:
            return 0

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        """Return 0-based row index where the column headers are."""
        for r in range(min(8, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if any(kw in row_text for kw in ("codigo", "nombre", "ceplan", "meta")):
                logger.debug(
                    "CuadroAoMetaParser: header row detected at %d", r
                )
                return r
        return 0


def _infer_ue_tipo(sigla: str, nombre: str) -> str:
    """Guess whether the UE is "CENTRAL" or "ODEI" from its sigla/name."""
    text = (sigla + " " + nombre).upper()
    if "ODEI" in text or "OFICINA DEPARTAMENTAL" in text or "REGIONAL" in text:
        return "ODEI"
    if "INEI" in text and "LIMA" not in text and "ODEI" not in text:
        return "CENTRAL"
    return "ODEI"
