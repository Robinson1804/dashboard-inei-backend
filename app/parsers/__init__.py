"""INEI Excel format parsers package.

Public API
----------
BaseParser        — Abstract base; inherit to create a new format parser.
ParseResult       — Dataclass returned by every ``parser.parse()`` call.
detect_format     — Auto-detect the format of an Excel file.

Concrete parsers (usable standalone):
    CuadroAoMetaParser      — Master hierarchy: UE → Meta → AO
    TablasParser            — ClasificadorGasto reference table (569 codes)
    Formato1Parser          — Annual budget programming (PIA/PIM + 12 months)
    Formato2Parser          — Task-level programming (19 cols + 12 months)
    Formato3Parser          — Tasks with justification text (no monthly cols)
    Formato5AParser         — AO monthly programming (programado only)
    Formato5BParser         — AO monthly execution (programado + ejecutado)
    Formato5ResumenParser   — AO execution summary (20 cols + monthly devengado)
    Formato04Parser         — Budget modifications (habilitaciones)
    Anexo01Parser           — HR staff register (DNI, cargo, remuneracion)

Format name constants (returned by ``detect_format``):
    FORMAT_CUADRO_AO_META, FORMAT_TABLAS, FORMAT_1, FORMAT_2, FORMAT_3,
    FORMAT_04, FORMAT_5A, FORMAT_5B, FORMAT_5_RESUMEN, FORMAT_ANEXO_01,
    FORMAT_SIAF, FORMAT_SIGA, FORMAT_UNKNOWN

Usage example::

    from app.parsers import detect_format, Formato1Parser

    fmt = detect_format("/path/to/file.xlsx")
    if fmt == "FORMATO_1":
        result = Formato1Parser("/path/to/file.xlsx").parse()
        print(result.summary())
        for rec in result.records:
            if rec["_type"] == "programacion_presupuestal":
                ...
"""

from .anexo01_parser import Anexo01Parser
from .base_parser import BaseParser, ParseResult
from .cuadro_ao_meta import CuadroAoMetaParser
from .detector import (
    FORMAT_04,
    FORMAT_1,
    FORMAT_2,
    FORMAT_3,
    FORMAT_5A,
    FORMAT_5B,
    FORMAT_5_RESUMEN,
    FORMAT_ANEXO_01,
    FORMAT_CUADRO_AO_META,
    FORMAT_SIAF,
    FORMAT_SIGA,
    FORMAT_TABLAS,
    FORMAT_UNKNOWN,
    KNOWN_FORMATS,
    detect_format,
)
from .formato04_parser import Formato04Parser
from .formato1_parser import Formato1Parser
from .formato2_parser import Formato2Parser
from .formato3_parser import Formato3Parser
from .formato5_resumen_parser import Formato5ResumenParser
from .formato5a_parser import Formato5AParser
from .formato5b_parser import Formato5BParser
from .tablas_parser import TablasParser

__all__: list[str] = [
    # Base
    "BaseParser",
    "ParseResult",
    # Detection
    "detect_format",
    "KNOWN_FORMATS",
    # Format name constants
    "FORMAT_CUADRO_AO_META",
    "FORMAT_TABLAS",
    "FORMAT_1",
    "FORMAT_2",
    "FORMAT_3",
    "FORMAT_04",
    "FORMAT_5A",
    "FORMAT_5B",
    "FORMAT_5_RESUMEN",
    "FORMAT_ANEXO_01",
    "FORMAT_SIAF",
    "FORMAT_SIGA",
    "FORMAT_UNKNOWN",
    # Concrete parsers
    "CuadroAoMetaParser",
    "TablasParser",
    "Formato1Parser",
    "Formato2Parser",
    "Formato3Parser",
    "Formato04Parser",
    "Formato5AParser",
    "Formato5BParser",
    "Formato5ResumenParser",
    "Anexo01Parser",
]
