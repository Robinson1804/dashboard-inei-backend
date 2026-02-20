"""
Application-wide constants for the Dashboard INEI system.

Defines domain enumerations, business rule thresholds, and
lookup lists used across routers, services, and models.
"""

from typing import Final

# ---------------------------------------------------------------------------
# User roles
# ---------------------------------------------------------------------------

ROLES: Final[list[str]] = [
    "ADMIN",
    "GERENCIA",
    "PRESUPUESTO",
    "LOGISTICA",
    "CONSULTA",
]

# ---------------------------------------------------------------------------
# Acquisition states (Adquisiciones >8 UIT)
# ---------------------------------------------------------------------------

ESTADOS_ADQUISICION: Final[list[str]] = [
    "EN_ACTOS_PREPARATORIOS",
    "EN_SELECCION",
    "EN_EJECUCION",
    "ADJUDICADO",
    "CULMINADO",
    "DESIERTO",
    "NULO",
]

# ---------------------------------------------------------------------------
# Minor contract states (Contratos Menores <=8 UIT)
# ---------------------------------------------------------------------------

ESTADOS_CONTRATO_MENOR: Final[list[str]] = [
    "PENDIENTE",
    "EN_PROCESO",
    "ORDEN_EMITIDA",
    "EJECUTADO",
    "PAGADO",
]

# ---------------------------------------------------------------------------
# Acquisition phases (Ley 32069)
# ---------------------------------------------------------------------------

FASES_ADQUISICION: Final[list[str]] = [
    "ACTUACIONES_PREPARATORIAS",
    "SELECCION",
    "EJECUCION_CONTRACTUAL",
]

# ---------------------------------------------------------------------------
# Object types
# ---------------------------------------------------------------------------

TIPOS_OBJETO: Final[list[str]] = [
    "BIEN",
    "SERVICIO",
    "OBRA",
    "CONSULTORIA",
]

# ---------------------------------------------------------------------------
# Procedure types
# ---------------------------------------------------------------------------

TIPOS_PROCEDIMIENTO: Final[list[str]] = [
    "LICITACION_PUBLICA",
    "CONCURSO_PUBLICO",
    "SUBASTA_INVERSA",
    "COMPARACION_PRECIOS",
    "CONTRATACION_DIRECTA",
    "CATALOGO_ELECTRONICO",
    "DIALOGO_COMPETITIVO",
]

# ---------------------------------------------------------------------------
# Alert levels (semaphore)
# ---------------------------------------------------------------------------

NIVELES_ALERTA: Final[list[str]] = [
    "ROJO",
    "AMARILLO",
    "VERDE",
]

# ---------------------------------------------------------------------------
# Application modules
# ---------------------------------------------------------------------------

MODULOS: Final[list[str]] = [
    "PRESUPUESTO",
    "ADQUISICIONES",
    "CONTRATOS_MENORES",
    "ACTIVIDADES_OPERATIVAS",
]

# ---------------------------------------------------------------------------
# Business rule thresholds (UIT 2026 = S/5,500)
# ---------------------------------------------------------------------------

UIT_2026: Final[int] = 5_500
UMBRAL_8_UIT: Final[int] = 44_000  # 8 × UIT_2026

# ---------------------------------------------------------------------------
# Execution traffic-light thresholds (Actividades Operativas)
# ---------------------------------------------------------------------------

SEMAFORO_VERDE_MIN: Final[float] = 0.90   # >= 90 % execution → green
SEMAFORO_AMARILLO_MIN: Final[float] = 0.70  # 70–89 % → yellow; < 70 % → red

# ---------------------------------------------------------------------------
# Fraccionamiento alert rules
# ---------------------------------------------------------------------------

FRACCIONAMIENTO_MAX_CONTRATOS_MES: Final[int] = 3    # >= 3 contracts same DDNNTT/category/month
FRACCIONAMIENTO_ACUMULADO_TRIMESTRE: Final[int] = UMBRAL_8_UIT  # > 8 UIT same object per quarter

# ---------------------------------------------------------------------------
# Stalled-process alert thresholds (days)
# ---------------------------------------------------------------------------

DIAS_PARALIZADO_ADQUISICION: Final[int] = 30
DIAS_PARALIZADO_CONTRATO: Final[int] = 15
