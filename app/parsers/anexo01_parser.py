"""Parser for Anexo 01 — Human Resources Data.

Sheet layout (rows are 1-based as in Excel):
    Row 1: Title
    Row 2: Blank or subtitle
    Row 3: UE name / code context
    Row 4: Fiscal year context
    Row 5: Blank / separator
    Row 6: Blank / separator
    Row 7: Column headers
    Row 8+: Data rows (F8 in Excel = index 7 in 0-based)

Expected ~12 columns:
    N° | DNI | Apellidos y Nombres | Cargo | Area |
    Regimen Laboral | Tipo Contrato |
    Fecha Inicio | Fecha Fin | Remuneracion Mensual |
    Observaciones | Estado

Context rows typically contain:
    UE name, UE codigo, and Fiscal Year.

Mapped to:
    - One ``personal_rrhh`` record per staff member row containing all
      identification, contractual, and compensation fields.

Validation rules:
    - DNI must be exactly 8 numeric digits.
    - remuneracion must be non-negative (0 is accepted for volunteer / ad-honorem).
    - nombre_completo must be non-empty.
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
    "ue_nombre": (2, 1),
    "ue_codigo": (2, 3),
    "anio":      (3, 3),
}

_DNI_RE = re.compile(r"^\d{8}$")

_COL_ALIASES: dict[str, list[str]] = {
    "numero": [
        "n°", "n", "num", "numero", "número", "item", "ítem", "#",
        "ord", "orden",
    ],
    "dni": [
        "dni", "d.n.i", "d.n.i.", "documento", "doc. identidad",
        "numero dni", "número dni",
    ],
    "nombre_completo": [
        "apellidos y nombres", "nombres y apellidos", "nombre completo",
        "apellidos nombres", "nombre", "trabajador", "personal",
        "servidor",
    ],
    "cargo": [
        "cargo", "puesto", "función", "funcion", "denominacion cargo",
        "denominación cargo",
    ],
    "area": [
        "area", "área", "unidad", "oficina", "dependencia",
        "unidad organica", "unidad orgánica",
    ],
    "regimen_laboral": [
        "regimen laboral", "régimen laboral", "regimen", "régimen",
        "reg. laboral", "modalidad laboral",
    ],
    "tipo_contrato": [
        "tipo contrato", "tipo de contrato", "modalidad contrato",
        "modalidad de contrato", "modalidad", "condicion",
        "condición laboral",
    ],
    "fecha_inicio": [
        "fecha inicio", "fecha de inicio", "fecha ingreso",
        "inicio contrato", "f. inicio",
    ],
    "fecha_fin": [
        "fecha fin", "fecha de fin", "fecha termino", "fecha término",
        "vencimiento", "fin contrato", "f. fin",
    ],
    "remuneracion": [
        "remuneracion mensual", "remuneración mensual",
        "remuneracion", "remuneración", "sueldo", "haber mensual",
        "haber", "monto mensual",
    ],
    "observaciones": [
        "observaciones", "observacion", "observación", "obs.",
        "comentarios", "comentario",
    ],
    "estado": [
        "estado", "condicion", "condición", "situacion", "situación",
        "activo", "vigente",
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


def _normalise_dni(raw: str) -> str:
    """Strip non-digit characters from a raw DNI cell value."""
    return re.sub(r"\D", "", raw.strip())


def _normalise_date(raw: str) -> str:
    """Attempt to normalise a date string to ISO format (YYYY-MM-DD).

    Common Peruvian date formats handled:
        DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, DD/MM/YY.

    Returns the original stripped string when parsing fails, so the
    record is still captured with the raw value for manual review.
    """
    if not raw:
        return ""
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            from datetime import datetime

            return datetime.strptime(raw.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return raw.strip()


class Anexo01Parser(BaseParser):
    """Parse Anexo 01 — human resources staff register.

    Each row represents one staff member assigned to the executing unit
    (DDNNTT) for the fiscal year indicated in the file header context.
    Fields cover personal identification (DNI), position (cargo / area),
    contractual information (regime, contract type, dates), and
    compensation (remuneracion mensual).

    Args:
        file_path_or_bytes: File path string, raw bytes, or binary file object.
        sheet_name: Sheet index or name.  Defaults to 0.
        data_start_row: 0-based row index where data rows begin.
            Defaults to 7 (row 8 in Excel).
    """

    FORMAT_NAME = "ANEXO_01"

    def __init__(
        self,
        file_path_or_bytes: str | bytes,
        sheet_name: str | int = 0,
        data_start_row: int = 7,
    ) -> None:
        super().__init__(file_path_or_bytes)
        self.sheet_name = sheet_name
        self.data_start_row = data_start_row

    # ------------------------------------------------------------------
    # Structure validation
    # ------------------------------------------------------------------

    def validate_structure(self, df: pd.DataFrame) -> list[str]:
        """Verify that DNI and nombre_completo columns are present."""
        errors: list[str] = []
        required_aliases = {
            "dni":            _COL_ALIASES["dni"],
            "nombre_completo": _COL_ALIASES["nombre_completo"],
        }
        for field, aliases in required_aliases.items():
            if _match_column(df, aliases) is None:
                errors.append(
                    f"Anexo01: columna '{field}' no encontrada. "
                    f"Columnas detectadas: {list(df.columns)}"
                )
        return errors

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        """Execute the Anexo 01 parsing pipeline."""
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
        if not context.get("ue_nombre"):
            context["ue_nombre"] = self._scan_for_value(raw_head, "unidad ejecutora")
        if not context.get("ue_codigo"):
            context["ue_codigo"] = self._scan_for_value(
                raw_head, "codigo ue", col_offset=1
            )

        self.result.metadata.update(context)

        try:
            anio = int(float(context.get("anio", "0") or "0"))
        except (ValueError, TypeError):
            anio = 0
            self.result.warnings.append(
                "Anexo01: no se pudo determinar el año; se usará 0."
            )

        ue_codigo = context.get("ue_codigo", "")
        ue_nombre = context.get("ue_nombre", "")

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
            self.result.errors.append("Anexo01: la hoja está vacía.")
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
        col_numero   = _match_column(df, _COL_ALIASES["numero"])
        col_dni      = _match_column(df, _COL_ALIASES["dni"])
        col_nombre   = _match_column(df, _COL_ALIASES["nombre_completo"])
        col_cargo    = _match_column(df, _COL_ALIASES["cargo"])
        col_area     = _match_column(df, _COL_ALIASES["area"])
        col_regimen  = _match_column(df, _COL_ALIASES["regimen_laboral"])
        col_tipo_con = _match_column(df, _COL_ALIASES["tipo_contrato"])
        col_f_inicio = _match_column(df, _COL_ALIASES["fecha_inicio"])
        col_f_fin    = _match_column(df, _COL_ALIASES["fecha_fin"])
        col_remun    = _match_column(df, _COL_ALIASES["remuneracion"])
        col_obs      = _match_column(df, _COL_ALIASES["observaciones"])
        col_estado   = _match_column(df, _COL_ALIASES["estado"])

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
            header_kws = ["dni", "remuneracion", "regimen", "nombre"]
            if self._is_header_row(row, header_kws):
                continue

            # ----------------------------------------------------------
            # DNI (required, must be 8 digits)
            # ----------------------------------------------------------
            raw_dni = self._clean_str(row.get(col_dni, "")) if col_dni else ""
            dni = _normalise_dni(raw_dni)

            if not _DNI_RE.match(dni):
                if dni:
                    self.result.warnings.append(
                        f"Fila {row_idx}: DNI inválido ('{raw_dni}' → '{dni}') "
                        f"— debe tener exactamente 8 dígitos — fila omitida."
                    )
                skipped += 1
                continue

            # ----------------------------------------------------------
            # nombre_completo (required)
            # ----------------------------------------------------------
            nombre_completo = (
                self._clean_str(row.get(col_nombre, "")) if col_nombre else ""
            )
            if not nombre_completo:
                self.result.warnings.append(
                    f"Fila {row_idx}: nombre_completo vacío para DNI '{dni}' "
                    f"— fila omitida."
                )
                skipped += 1
                continue

            # ----------------------------------------------------------
            # Optional text fields
            # ----------------------------------------------------------
            numero       = self._clean_str(row.get(col_numero, ""))   if col_numero   else ""
            cargo        = self._clean_str(row.get(col_cargo, ""))    if col_cargo    else ""
            area         = self._clean_str(row.get(col_area, ""))     if col_area     else ""
            regimen      = self._clean_str(row.get(col_regimen, ""))  if col_regimen  else ""
            tipo_contrato = (
                self._clean_str(row.get(col_tipo_con, "")) if col_tipo_con else ""
            )
            observaciones = (
                self._clean_str(row.get(col_obs, "")) if col_obs else ""
            )
            estado = self._clean_str(row.get(col_estado, "")) if col_estado else ""

            # ----------------------------------------------------------
            # Date fields (normalised to ISO when possible)
            # ----------------------------------------------------------
            fecha_inicio = (
                _normalise_date(self._clean_str(row.get(col_f_inicio, "")))
                if col_f_inicio else ""
            )
            fecha_fin = (
                _normalise_date(self._clean_str(row.get(col_f_fin, "")))
                if col_f_fin else ""
            )

            # ----------------------------------------------------------
            # Remuneracion (float, non-negative)
            # ----------------------------------------------------------
            remuneracion = (
                self._to_decimal(row.get(col_remun)) if col_remun else 0.0
            )
            if remuneracion < 0:
                self.result.warnings.append(
                    f"Fila {row_idx} DNI '{dni}': remuneracion negativa "
                    f"({remuneracion:.2f}) — se usará 0."
                )
                remuneracion = 0.0

            # ----------------------------------------------------------
            # Emit personal_rrhh record
            # ----------------------------------------------------------
            record: dict[str, Any] = {
                "_type": "personal_rrhh",
                "anio": anio,
                "ue_codigo": ue_codigo,
                "ue_nombre": ue_nombre,
                "numero": numero,
                "dni": dni,
                "nombre_completo": nombre_completo,
                "cargo": cargo,
                "area": area,
                "regimen_laboral": regimen,
                "tipo_contrato": tipo_contrato,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
                "remuneracion": round(remuneracion, 2),
                "observaciones": observaciones,
                "estado": estado,
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
                "ue_nombre": ue_nombre,
            }
        )

        logger.info(
            "Anexo01Parser: rows=%d skipped=%d anio=%d ue=%s",
            valid_rows,
            skipped,
            anio,
            ue_codigo,
        )
        return self.result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _detect_header_row(self, raw_head: pd.DataFrame) -> int:
        """Find the 0-based row index containing 'dni', 'remuneracion', or 'regimen'."""
        for r in range(min(10, len(raw_head))):
            row_text = " ".join(
                self._clean_str(v).lower() for v in raw_head.iloc[r]
            )
            if "dni" in row_text:
                return r
            if "remuneracion" in row_text or "remuneración" in row_text:
                return r
            if "régimen" in row_text or "regimen" in row_text:
                return r
        return max(0, self.data_start_row - 1)
