"""Parser for Formato 3 — Tasks with Justification.

Sheet layout (rows are 1-based as in Excel):
    Row 1: Title
    Row 2: Blank or subtitle
    Row 3: UE name / code context
    Row 4: Meta context
    Row 5: Year context
    Row 6: Blank / separator
    Row 7: Column headers
    Row 8+: Data rows (F8 in Excel = index 7 in 0-based)

Expected ~15 columns:
    Cod Meta | Desc Meta | Cod AO | Desc AO | Cod Tarea | Desc Tarea |
    Clasificador | Desc Clasificador | PIM |
    Programado | Ejecutado | Saldo | % Avance |
    Justificacion | Observaciones

Unlike Formato 2 there are no per-month columns; instead the format
provides consolidated execution figures plus free-text justification
and observation fields for each task/classifier row.

Mapped to:
    - One ``ProgramacionPresupuestal`` record per data row containing
      all financial fields and the justification text.

Validation rules:
    - Clasificador code must match the ``X.X…`` pattern.
    - PIM must be non-negative.
    - Saldo ≈ PIM − ejecutado (tolerance ±1 sol).
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
    "programado": ["programado", "programación", "programacion"],
    "ejecutado": ["ejecutado", "ejecucion", "ejecución", "devengado"],
    "saldo": ["saldo", "saldo disponible", "saldo por ejecutar"],
    "pct_avance": [
        "% avance", "% de avance", "avance", "porcentaje avance",
        "porcentaje de avance", "% ejec", "% ejecucion",
    ],
    "justificacion": [
        "justificacion", "justificación", "justif.",
        "motivo", "sustento",
    ],
    "observaciones": [
        "observaciones", "observacion", "observación", "obs.",
        "comentarios", "comentario",
    ],
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


class Formato3Parser(BaseParser):
    """Parse Formato 3 — tasks with execution summary and justification text.

    This format is similar to Formato 2 in its hierarchical structure but
    replaces the 12 monthly columns with consolidated execution figures
    (programado, ejecutado, saldo, % avance) and adds free-text
    justification and observation columns for narrative reporting.

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 7 (row 8 in Excel).
        context_start_row: 0-based first row of context/header area.
            Defaults to 0.
    """

    FORMAT_NAME = "FORMATO_3"

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
            "clasificador":  _COL_ALIASES["clasificador"],
            "pim":           _COL_ALIASES["pim"],
            "justificacion": _COL_ALIASES["justificacion"],
        }
        for field, aliases in required_aliases.items():
            if _match_column(df, aliases) is None:
                errors.append(
                    f"Formato3: columna '{field}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Formato 3 parsing pipeline."""
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
                "Formato3: no se pudo determinar el año del contexto; se usará 0."
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
            self.result.errors.append("Formato3: la hoja está vacía.")
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
        col_cod_meta    = _match_column(df, _COL_ALIASES["cod_meta"])
        col_desc_meta   = _match_column(df, _COL_ALIASES["desc_meta"])
        col_cod_ao      = _match_column(df, _COL_ALIASES["cod_ao"])
        col_desc_ao     = _match_column(df, _COL_ALIASES["desc_ao"])
        col_cod_tarea   = _match_column(df, _COL_ALIASES["cod_tarea"])
        col_desc_tarea  = _match_column(df, _COL_ALIASES["desc_tarea"])
        col_clas        = _match_column(df, _COL_ALIASES["clasificador"])
        col_desc_clas   = _match_column(df, _COL_ALIASES["desc_clasificador"])
        col_pim         = _match_column(df, _COL_ALIASES["pim"])
        col_programado  = _match_column(df, _COL_ALIASES["programado"])
        col_ejecutado   = _match_column(df, _COL_ALIASES["ejecutado"])
        col_saldo       = _match_column(df, _COL_ALIASES["saldo"])
        col_pct         = _match_column(df, _COL_ALIASES["pct_avance"])
        col_justif      = _match_column(df, _COL_ALIASES["justificacion"])
        col_obs         = _match_column(df, _COL_ALIASES["observaciones"])

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
            header_kws = ["clasificador", "pim", "tarea", "justificacion"]
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

            effective_meta = cod_meta or meta_codigo

            # ----------------------------------------------------------
            # Financial amounts
            # ----------------------------------------------------------
            pim        = self._to_decimal(row.get(col_pim))       if col_pim       else 0.0
            programado = self._to_decimal(row.get(col_programado)) if col_programado else 0.0
            ejecutado  = self._to_decimal(row.get(col_ejecutado))  if col_ejecutado  else 0.0
            saldo_decl = self._to_decimal(row.get(col_saldo))      if col_saldo      else None
            pct_avance = self._to_decimal(row.get(col_pct))        if col_pct        else None

            if pim < 0:
                self.result.warnings.append(
                    f"Fila {row_idx}: PIM negativo ({pim:.2f}) "
                    f"para clasificador '{clasificador}' — fila omitida."
                )
                skipped += 1
                continue

            # Validate saldo ≈ PIM − ejecutado
            computed_saldo = round(pim - ejecutado, 2)
            if saldo_decl is not None and abs(computed_saldo - round(saldo_decl, 2)) > 1.0:
                self.result.warnings.append(
                    f"Fila {row_idx} clasificador '{clasificador}': "
                    f"saldo declarado ({saldo_decl:.2f}) ≠ calculado "
                    f"(PIM−Ejecutado = {computed_saldo:.2f})."
                )

            saldo_final = saldo_decl if saldo_decl is not None else computed_saldo

            # ----------------------------------------------------------
            # Text fields
            # ----------------------------------------------------------
            justificacion = self._clean_str(row.get(col_justif, "")) if col_justif else ""
            observaciones = self._clean_str(row.get(col_obs, ""))    if col_obs    else ""

            # ----------------------------------------------------------
            # Emit record
            # ----------------------------------------------------------
            record: dict[str, Any] = {
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
                "programado": round(programado, 2),
                "ejecutado": round(ejecutado, 2),
                "saldo": round(saldo_final, 2),
                "pct_avance": round(pct_avance, 4) if pct_avance is not None else None,
                "justificacion": justificacion,
                "observaciones": observaciones,
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
            "Formato3Parser: rows=%d skipped=%d anio=%d ue=%s meta=%s",
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
        """Find the 0-based row index containing 'justificacion' or 'tarea'."""
        for r in range(min(10, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if "justificacion" in row_text or "justificación" in row_text:
                return r
            if "tarea" in row_text:
                return r
        return max(0, self.data_start_row - 1)
