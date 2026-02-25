"""Seed 2025 completo — Presupuesto, Adquisiciones, Contratos Menores, AO.

Reemplaza a seed_2025.py. Siembra datos completos del año 2025 cubriendo
los 4 modulos del Dashboard INEI:
  - Presupuesto (ProgramacionPresupuestal + ProgramacionMensual)
  - Adquisiciones >8 UIT (22 adquisiciones + 22 hitos c/u)
  - Contratos Menores <=8 UIT (40 contratos + 9 hitos c/u)
  - Actividades Operativas (30 AOs distribuidas por UE)

El script es idempotente: si ya existen datos 2025, omite la ejecucion.
Ejecutar con --force para resetear y re-sembrar.

Ejecucion:
    cd backend
    python seed_2025_completo.py
    python seed_2025_completo.py --force
"""

from __future__ import annotations

import sys
import os
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal
from app.models import (
    UnidadEjecutora,
    MetaPresupuestal,
    ClasificadorGasto,
    ProgramacionPresupuestal,
    ProgramacionMensual,
    Adquisicion,
    AdquisicionDetalle,
    AdquisicionProceso,
    ContratoMenor,
    ContratoMenorProceso,
    ActividadOperativa,
    Proveedor,
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

ANIO = 2025
UIT = 5450  # UIT 2025 = S/ 5,450
LIMITE_CM = UIT * 8  # S/ 43,600 — tope contratos menores

MES_PESOS = [0.04, 0.06, 0.08, 0.09, 0.10, 0.10, 0.10, 0.10, 0.09, 0.10, 0.08, 0.06]
assert abs(sum(MES_PESOS) - 1.0) < 1e-9, "MES_PESOS no suma 1.0"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------


def _dec(valor: float) -> Decimal:
    """Convierte float a Decimal con 2 decimales (redondeo HALF_UP)."""
    return Decimal(str(valor)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _d(y: int, m: int, d_: int) -> date:
    """Crea un objeto date con argumentos posicionales."""
    return date(y, m, d_)


def _fuente_financiamiento(sigla: str, indice: int) -> str:
    """Determina la fuente de financiamiento segun tipo de UE."""
    return "Recursos Ordinarios"


def _calcular_montos(
    pim: float, pct_ejecucion: float, pct_clasif: float
) -> dict[str, Decimal]:
    """Calcula montos presupuestales para un clasificador."""
    pim_clasif = pim * pct_clasif
    devengado = pim_clasif * (pct_ejecucion / 100.0)
    return {
        "pia": _dec(pim_clasif * 0.97),
        "pim": _dec(pim_clasif),
        "devengado": _dec(devengado),
        "certificado": _dec(devengado * 1.02),
        "compromiso_anual": _dec(devengado * 1.01),
        "girado": _dec(devengado * 0.98),
        "saldo": _dec(pim_clasif - devengado),
    }


def _calcular_meses(
    pim_clasif: Decimal, devengado_total: Decimal
) -> list[tuple[Decimal, Decimal, Decimal]]:
    """Genera 12 registros mensuales (programado, ejecutado, saldo)."""
    programado_mes = _dec(float(pim_clasif) / 12.0)
    meses: list[tuple[Decimal, Decimal, Decimal]] = []
    ejecutado_acumulado = Decimal("0.00")

    for i, peso in enumerate(MES_PESOS):
        if i == 11:
            ejecutado_mes = devengado_total - ejecutado_acumulado
        else:
            ejecutado_mes = _dec(float(devengado_total) * peso)
        ejecutado_acumulado += ejecutado_mes
        saldo_mes = programado_mes - ejecutado_mes
        meses.append((programado_mes, ejecutado_mes, saldo_mes))

    return meses


# ---------------------------------------------------------------------------
# Datos maestros — UEs
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Datos maestros — Clasificadores de gasto
# ---------------------------------------------------------------------------

CLASIFS_DATOS = [
    ("2.3.1.5.1.2",   "Papeleria y utiles de oficina",         "2.3"),
    ("2.3.2.2.2.3",   "Equipos de computo y accesorios",       "2.3"),
    ("2.3.2.7.11.99", "Otros bienes de tecnologia",            "2.3"),
    ("2.3.1.99.1.99", "Otros bienes de consumo",               "2.3"),
    ("2.3.2.7.2.99",  "Software y licencias informaticas",     "2.3"),
    ("2.3.2.4.1.1",   "Mantenimiento de equipos de computo",   "2.3"),
    ("2.3.2.8.1.1",   "Servicios de comunicaciones",           "2.3"),
    ("2.3.2.4.1.99",  "Mantenimiento de infraestructura",      "2.3"),
    ("2.6.3.2.3.99",  "Equipamiento y mobiliario",             "2.6"),
    ("2.3.2.1.2.1",   "Servicio de limpieza",                  "2.3"),
    ("2.3.2.1.2.2",   "Servicio de seguridad y vigilancia",    "2.3"),
    ("2.3.2.5.1.1",   "Servicio de impresion y publicacion",   "2.3"),
    ("2.3.2.3.1.1",   "Servicio de consultoria",               "2.3"),
    ("2.3.2.9.1.1",   "Pasajes y gastos de transporte",        "2.3"),
    ("2.3.2.9.2.1",   "Viaticos y asignaciones",               "2.3"),
    ("2.3.1.3.1.1",   "Combustibles y carburantes",            "2.3"),
    ("2.3.1.1.1.1",   "Alimentos y bebidas para consumo",      "2.3"),
    ("2.3.1.2.1.1",   "Vestuario uniformes y prendas",         "2.3"),
]

# ---------------------------------------------------------------------------
# Presupuesto — PIM 2025 por sigla de UE
# ---------------------------------------------------------------------------

UE_BUDGET_2025: dict[str, int] = {
    "OTIN":     18_200_000,
    "DEC":      22_500_000,
    "OTA":       8_100_000,
    "OTPP":      6_300_000,
    "DNCPP":     4_800_000,
    "DNCE":      5_200_000,
    "DNEL":      3_900_000,
    "DTI":       4_100_000,
}

UE_EJECUCION_PCT: dict[str, float] = {
    "OTIN":  97.2,
    "DEC":   94.8,
    "OTA":   91.5,
    "OTPP":  88.3,
    "DNCPP": 95.1,
    "DNCE":  86.7,
    "DNEL":  82.4,
    "DTI":   90.6,
}

# Clasificadores por grupo de UE (codigo, porcentaje del PIM)
CLASIFS_OTIN: list[tuple[str, float]] = [
    ("2.3.2.2.2.3",   0.38),
    ("2.3.2.7.2.99",  0.25),
    ("2.3.2.7.11.99", 0.15),
    ("2.3.2.4.1.1",   0.12),
    ("2.3.2.8.1.1",   0.07),
    ("2.3.1.5.1.2",   0.03),
]

CLASIFS_ESTADISTICA: list[tuple[str, float]] = [
    ("2.3.1.1.1.1",  0.20),
    ("2.3.2.9.1.1",  0.25),
    ("2.3.2.9.2.1",  0.20),
    ("2.3.2.5.1.1",  0.15),
    ("2.3.2.3.1.1",  0.20),
]

CLASIFS_ORGANIZACION: list[tuple[str, float]] = [
    ("2.3.2.3.1.1",  0.30),
    ("2.3.1.5.1.2",  0.15),
    ("2.3.2.5.1.1",  0.25),
    ("2.3.2.9.2.1",  0.30),
]

CLASIFS_DTI: list[tuple[str, float]] = [
    ("2.6.3.2.3.99",  0.40),
    ("2.3.2.4.1.99",  0.25),
    ("2.3.2.2.2.3",   0.20),
    ("2.3.2.8.1.1",   0.10),
    ("2.3.1.5.1.2",   0.05),
]

UE_CLASIFS_MAP: dict[str, list[tuple[str, float]]] = {
    "OTIN":  CLASIFS_OTIN,
    "DEC":   CLASIFS_ESTADISTICA,
    "DNCE":  CLASIFS_ESTADISTICA,
    "DNEL":  CLASIFS_ESTADISTICA,
    "DNCPP": CLASIFS_ESTADISTICA,
    "OTA":   CLASIFS_ORGANIZACION,
    "OTPP":  CLASIFS_ORGANIZACION,
    "DTI":   CLASIFS_DTI,
}

# ---------------------------------------------------------------------------
# Proveedores
# ---------------------------------------------------------------------------

PROVEEDORES_DATOS = [
    # (ruc, razon_social, nombre_comercial)
    ("20100070970", "IBM DEL PERU S.A.C.",                         "IBM Peru"),
    ("20112273922", "MICROSOFT PERU S.R.L.",                       "Microsoft Peru"),
    ("20501503893", "SUMINISTROS PAPELEROS LA UNION S.A.C.",       "Papeleria La Union"),
    ("20602913771", "SOLUCIONES TECNOLOGICAS ANDINAS S.A.C.",      "STA Soluciones TI"),
    ("20536987412", "CONSULTORA ESTADISTICA PERU S.A.C.",          "CEP Consultores"),
    ("20600347851", "ELECTRO SERVICIOS GENERALES J&M E.I.R.L.",   "Electro J&M"),
    ("20490920850", "CORPORACION LINDLEY S.A.",                    "Corporacion Lindley"),
    ("20382036655", "PERUANA DE FUMIGACION S.A.C.",                "Fumigacion Peru"),
    ("20515396635", "SEGURIDAD Y VIGILANCIA ANDINA S.A.C.",        "SVA Seguridad"),
    ("20100022694", "TELEFONICA DEL PERU S.A.A.",                  "Telefonica Peru"),
    ("20421456835", "CLARO PERU S.A.",                             "Claro Peru"),
    ("20544082224", "LENOVO PERU S.A.C.",                          "Lenovo Peru"),
    ("20601234567", "IMPRESIONES GRAFICAS DEL PERU S.A.C.",        "Impresiones Peru"),
    ("20512345678", "DESARROLLO DE SOFTWARE Y SISTEMAS S.A.C.",    "DevSoft Peru"),
    ("20498765432", "CAPACITACION Y CONSULTORIA EMPRESARIAL S.A.C.", "Capacita Peru"),
]

# ---------------------------------------------------------------------------
# Hitos del proceso de adquisicion (22 hitos Ley 32069)
# Tuplas: (orden, hito, fase, area_responsable, dias_planificados)
# ---------------------------------------------------------------------------

HITOS_ADQ: list[tuple[int, str, str, str, int]] = [
    # FASE ACTUACIONES_PREPARATORIAS (hitos 1-9)
    (1,  "Elaboracion del Requerimiento Tecnico",        "ACTUACIONES_PREPARATORIAS", "OTIN",      5),
    (2,  "Conformidad del Area Usuaria",                 "ACTUACIONES_PREPARATORIAS", "OTIN",      3),
    (3,  "Elaboracion del Estudio de Mercado",           "ACTUACIONES_PREPARATORIAS", "DEC",       7),
    (4,  "Determinacion del Valor Referencial",          "ACTUACIONES_PREPARATORIAS", "DEC",       3),
    (5,  "Elaboracion del Expediente de Contratacion",   "ACTUACIONES_PREPARATORIAS", "OTA",       5),
    (6,  "Aprobacion del Expediente de Contratacion",    "ACTUACIONES_PREPARATORIAS", "OTPP",      3),
    (7,  "Designacion del Comite de Seleccion",          "ACTUACIONES_PREPARATORIAS", "OTPP",      2),
    (8,  "Elaboracion y Aprobacion de Bases",            "ACTUACIONES_PREPARATORIAS", "COMITE",    7),
    (9,  "Aprobacion de Bases por SEACE",                "ACTUACIONES_PREPARATORIAS", "OTIN",      2),
    # FASE SELECCION (hitos 10-17)
    (10, "Convocatoria y Publicacion en SEACE",          "SELECCION",                 "COMITE",    1),
    (11, "Registro de Participantes",                    "SELECCION",                 "COMITE",    5),
    (12, "Formulacion de Consultas y Observaciones",     "SELECCION",                 "COMITE",    5),
    (13, "Absolucion de Consultas y Observaciones",      "SELECCION",                 "COMITE",    7),
    (14, "Integracion de Bases",                         "SELECCION",                 "COMITE",    3),
    (15, "Presentacion de Ofertas",                      "SELECCION",                 "PROVEEDOR", 5),
    (16, "Evaluacion y Calificacion de Ofertas",         "SELECCION",                 "COMITE",    7),
    (17, "Otorgamiento de Buena Pro",                    "SELECCION",                 "COMITE",    3),
    # FASE EJECUCION_CONTRACTUAL (hitos 18-22)
    (18, "Suscripcion de Contrato",                      "EJECUCION_CONTRACTUAL",     "OTIN",      5),
    (19, "Entrega de Adelanto (si aplica)",              "EJECUCION_CONTRACTUAL",     "DEC",       10),
    (20, "Ejecucion del Contrato / Prestacion del Servicio", "EJECUCION_CONTRACTUAL", "PROVEEDOR", 30),
    (21, "Conformidad de la Prestacion",                 "EJECUCION_CONTRACTUAL",     "OTA",       5),
    (22, "Pago al Proveedor",                            "EJECUCION_CONTRACTUAL",     "DEC",       7),
]

# ---------------------------------------------------------------------------
# Hitos del proceso de contrato menor (9 pasos)
# Tuplas: (orden, hito, area_responsable, dias_planificados)
# ---------------------------------------------------------------------------

HITOS_CM: list[tuple[int, str, str, int]] = [
    (1, "Elaboracion del Requerimiento",         "LOGISTICA",  1),
    (2, "Autorizacion del Gasto (CCP)",          "OTA",        1),
    (3, "Solicitud de Cotizaciones",             "LOGISTICA",  3),
    (4, "Recepcion de Cotizaciones",             "LOGISTICA",  3),
    (5, "Cuadro Comparativo de Cotizaciones",    "LOGISTICA",  1),
    (6, "Emision de Orden de Compra/Servicio",   "LOGISTICA",  1),
    (7, "Notificacion al Proveedor",             "LOGISTICA",  1),
    (8, "Entrega / Prestacion del Servicio",     "PROVEEDOR",  7),
    (9, "Conformidad y Pago",                    "OTA",        5),
]

# ---------------------------------------------------------------------------
# Adquisiciones 2025 (22 registros)
# Tuplas: (seq, ue_sigla, descripcion, tipo_objeto, tipo_procedimiento, estado, monto_ref)
# ---------------------------------------------------------------------------

ADQ_DATA: list[tuple[int, str, str, str, str, str, float]] = [
    (1,  "OTIN",     "Adquisicion de Servidores de Alta Disponibilidad",          "BIEN",     "LICITACION_PUBLICA",   "CULMINADO",              850_000.00),
    (2,  "OTIN",     "Servicio de Mantenimiento de Red LAN/WAN",                  "SERVICIO", "CONCURSO_PUBLICO",     "CULMINADO",              320_000.00),
    (3,  "DEC",      "Impresion de Material Censal 2025",                          "BIEN",     "LICITACION_PUBLICA",   "CULMINADO",            1_200_000.00),
    (4,  "DEC",      "Servicio de Encuestadores para Encuesta Nacional",           "SERVICIO", "CONCURSO_PUBLICO",     "CULMINADO",            2_800_000.00),
    (5,  "OTA",      "Consultoria para Modernizacion Administrativa",              "SERVICIO", "CONCURSO_PUBLICO",     "CULMINADO",              480_000.00),
    (6,  "OTPP",     "Adquisicion de Equipos de Oficina",                          "BIEN",     "COMPARACION_PRECIOS",  "CULMINADO",               95_000.00),
    (7,  "DNCE",     "Servicio de Publicacion de Informes Estadisticos",           "SERVICIO", "CONCURSO_PUBLICO",     "CULMINADO",              380_000.00),
    (8,  "DNCPP",    "Adquisicion de Tablets para Empadronadores",                 "BIEN",     "SUBASTA_INVERSA",      "CULMINADO",              650_000.00),
    (9,  "DTI",      "Adquisicion de Equipos de Seguridad Electronica",            "BIEN",     "LICITACION_PUBLICA",   "ADJUDICADO",             420_000.00),
    (10, "DNEL",     "Servicio de Consultoria en Estadisticas Laborales",          "SERVICIO", "CONCURSO_PUBLICO",     "CULMINADO",              280_000.00),
    (11, "OTIN",     "Adquisicion de Mobiliario de Oficina Institucional",         "BIEN",     "COMPARACION_PRECIOS",  "CULMINADO",              185_000.00),
    (12, "DEC",      "Servicio de Mantenimiento de Vehiculos Institucionales",      "SERVICIO", "COMPARACION_PRECIOS",  "CULMINADO",              125_000.00),
    (13, "DTI",      "Adquisicion de Equipos de Computo para Sede Central",         "BIEN",     "SUBASTA_INVERSA",      "CULMINADO",              290_000.00),
    (14, "OTA",      "Servicio de Limpieza y Mantenimiento de Edificio",            "SERVICIO", "COMPARACION_PRECIOS",  "CULMINADO",               98_000.00),
    (15, "OTPP",     "Adquisicion de Material de Escritorio Institucional",         "BIEN",     "COMPARACION_PRECIOS",  "CULMINADO",               75_000.00),
    (16, "DNCE",     "Consultoria para Diseno de Informes Estadisticos",            "SERVICIO", "CONCURSO_PUBLICO",     "ADJUDICADO",             340_000.00),
    (17, "DNCPP",    "Adquisicion de Vehiculo para Operaciones de Campo",           "BIEN",     "LICITACION_PUBLICA",   "CULMINADO",              155_000.00),
    (18, "DNEL",     "Servicio de Seguridad y Vigilancia Institucional",            "SERVICIO", "COMPARACION_PRECIOS",  "CULMINADO",               88_000.00),
    (19, "DTI",      "Adquisicion de Equipos de Medicion y Calibracion",            "BIEN",     "COMPARACION_PRECIOS",  "ADJUDICADO",             210_000.00),
    (20, "OTA",      "Servicio de Capacitacion para Personal Administrativo",       "SERVICIO", "CONCURSO_PUBLICO",     "DESIERTO",               145_000.00),
    (21, "OTIN",     "Adquisicion de Software de Gestion Documental",               "BIEN",     "SUBASTA_INVERSA",      "CULMINADO",               95_000.00),
    (22, "DEC",      "Servicio de Consultoria en Tecnologias de Informacion",       "SERVICIO", "CONCURSO_PUBLICO",     "EN_SELECCION",           520_000.00),
]

# Mapeo estado → fase_actual
ESTADO_FASE_MAP: dict[str, str] = {
    "EN_ACTOS_PREPARATORIOS": "ACTUACIONES_PREPARATORIAS",
    "EN_SELECCION":           "SELECCION",
    "EN_EJECUCION":           "EJECUCION_CONTRACTUAL",
    "ADJUDICADO":             "EJECUCION_CONTRACTUAL",
    "CULMINADO":              "EJECUCION_CONTRACTUAL",
    "DESIERTO":               "SELECCION",
    "NULO":                   "SELECCION",
}

# Fechas base de inicio segun estado
ESTADO_FECHA_BASE: dict[str, date] = {
    "CULMINADO":              _d(2025, 1, 5),
    "ADJUDICADO":             _d(2025, 2, 1),
    "EN_SELECCION":           _d(2025, 3, 1),
    "EN_ACTOS_PREPARATORIOS": _d(2025, 10, 1),
    "DESIERTO":               _d(2025, 4, 1),
    "EN_EJECUCION":           _d(2025, 2, 1),
    "NULO":                   _d(2025, 4, 1),
}

# ---------------------------------------------------------------------------
# Contratos menores 2025 (40 registros)
# Tuplas: (ue_sigla, descripcion, tipo_objeto, categoria, estado, monto_est, monto_ejec, n_cotiz)
# monto_ejec = None si estado es ORDEN_EMITIDA/EN_PROCESO/PENDIENTE
# ---------------------------------------------------------------------------

CM_DATA: list[tuple[str, str, str, str, str, float, float | None, int]] = [
    ("OTIN",     "Suministro de Toner y Cartuchos de Impresion",           "BIEN",     "SUMINISTROS TI",   "PAGADO",        3800.00,  3750.00,  3),
    ("OTIN",     "Servicio de Soporte Tecnico Informatico",                 "SERVICIO", "SOPORTE TI",       "PAGADO",       12000.00, 11800.00,  2),
    ("OTIN",     "Adquisicion de Mouse, Teclados y Accesorios",             "BIEN",     "SUMINISTROS TI",   "PAGADO",        2400.00,  2380.00,  3),
    ("OTIN",     "Servicio de Configuracion de Switch de Red",              "SERVICIO", "SOPORTE TI",       "PAGADO",        8500.00,  8350.00,  2),
    ("OTIN",     "Adquisicion de Memorias RAM y Discos SSD",               "BIEN",     "SUMINISTROS TI",   "EJECUTADO",    15000.00, 14800.00,  3),
    ("DEC",      "Impresion de Encuestas y Formularios",                    "BIEN",     "IMPRESION",        "PAGADO",        9800.00,  9650.00,  3),
    ("DEC",      "Servicio de Fotocopiado Masivo",                          "SERVICIO", "IMPRESION",        "PAGADO",        6200.00,  6100.00,  2),
    ("DEC",      "Adquisicion de Lapiceros y Material de Campo",            "BIEN",     "MATERIAL CAMPO",   "PAGADO",        3400.00,  3350.00,  3),
    ("DEC",      "Servicio de Transporte de Encuestadores",                 "SERVICIO", "TRANSPORTE",       "PAGADO",       18000.00, 17600.00,  2),
    ("DEC",      "Adquisicion de Tableros y Clipboards",                    "BIEN",     "MATERIAL CAMPO",   "PAGADO",        2800.00,  2750.00,  3),
    ("DEC",      "Servicio de Catering para Capacitacion",                  "SERVICIO", "ALIMENTACION",     "PAGADO",        7500.00,  7400.00,  2),
    ("OTA",      "Adquisicion de Archivadores y Folder Manila",             "BIEN",     "UTILES OFICINA",   "PAGADO",        1800.00,  1780.00,  3),
    ("OTA",      "Servicio de Courier y Mensajeria",                        "SERVICIO", "SERVICIOS ADMIN",  "PAGADO",        4200.00,  4100.00,  2),
    ("OTA",      "Adquisicion de Papel Bond A4 y A3",                       "BIEN",     "UTILES OFICINA",   "EJECUTADO",     6500.00,  6400.00,  3),
    ("OTPP",     "Servicio de Impresion de Informes Anuales",               "SERVICIO", "IMPRESION",        "PAGADO",       11000.00, 10800.00,  2),
    ("OTPP",     "Adquisicion de Agenda y Material de Planificacion",       "BIEN",     "UTILES OFICINA",   "PAGADO",        2200.00,  2180.00,  3),
    ("DNCE",     "Servicio de Encuadernacion de Publicaciones",             "SERVICIO", "IMPRESION",        "PAGADO",        5500.00,  5400.00,  2),
    ("DNCE",     "Adquisicion de USB y Medios de Almacenamiento",           "BIEN",     "SUMINISTROS TI",   "PAGADO",        3200.00,  3150.00,  3),
    ("DNCPP",    "Servicio de Lavanderia y Limpieza de Uniformes",          "SERVICIO", "SERVICIOS ADMIN",  "PAGADO",        4800.00,  4700.00,  2),
    ("DNCPP",    "Adquisicion de Chalecos y Gorras para Campo",             "BIEN",     "VESTUARIO",        "PAGADO",        8900.00,  8800.00,  3),
    ("DNCPP",    "Adquisicion de Mochilas para Encuestadores",              "BIEN",     "MATERIAL CAMPO",   "EJECUTADO",    12000.00, 11900.00,  3),
    ("DNEL",     "Servicio de Traduccion de Informes Tecnicos",             "SERVICIO", "SERVICIOS ADMIN",  "PAGADO",        6000.00,  5900.00,  2),
    ("DNEL",     "Adquisicion de Libros y Publicaciones",                   "BIEN",     "MATERIAL BIBLIOG", "PAGADO",        3500.00,  3450.00,  3),
    ("DTI",      "Servicio de Mantenimiento de UPS y Baterias",             "SERVICIO", "MANTENIMIENTO",    "PAGADO",        9000.00,  8850.00,  2),
    ("DTI",      "Adquisicion de Cables y Conectores de Red",               "BIEN",     "SUMINISTROS TI",   "PAGADO",        2100.00,  2080.00,  3),
    ("OTIN",     "Adquisicion de Combustible para Vehiculos Institucionales",  "BIEN",     "COMBUSTIBLE",      "PAGADO",       18000.00, 17800.00,  3),
    ("OTIN",     "Servicio de Limpieza de Oficinas Sede Central",             "SERVICIO", "SERVICIOS ADMIN",  "EJECUTADO",    12000.00, 11800.00,  2),
    ("DEC",      "Adquisicion de Material de Escritorio para DEC",            "BIEN",     "UTILES OFICINA",   "PAGADO",        4200.00,  4150.00,  3),
    ("DEC",      "Servicio de Mantenimiento de Fotocopiadoras DEC",           "SERVICIO", "MANTENIMIENTO",    "PAGADO",        3800.00,  3750.00,  2),
    ("OTA",      "Adquisicion de Papel y Consumibles de Impresion",           "BIEN",     "UTILES OFICINA",   "PAGADO",        5600.00,  5500.00,  3),
    ("OTA",      "Servicio de Mensajeria Institucional",                      "SERVICIO", "TRANSPORTE",       "EJECUTADO",     8000.00,  7850.00,  2),
    ("OTPP",     "Adquisicion de Suministros para Operaciones de Campo",      "BIEN",     "MATERIAL CAMPO",   "PAGADO",        7200.00,  7100.00,  3),
    ("DNCE",     "Adquisicion de Equipo Menor de Oficina DNCE",               "BIEN",     "UTILES OFICINA",   "PAGADO",        3900.00,  3850.00,  3),
    ("DNCPP",    "Servicio de Limpieza para DNCPP",                           "SERVICIO", "SERVICIOS ADMIN",  "PAGADO",        9000.00,  8900.00,  2),
    ("DNEL",     "Adquisicion de Combustible para DNEL",                      "BIEN",     "COMBUSTIBLE",      "PAGADO",       14000.00, 13800.00,  3),
    ("DTI",      "Servicio de Soporte Tecnico Especializado",                 "SERVICIO", "SOPORTE TI",       "EJECUTADO",     5500.00,  5400.00,  2),
    ("OTIN",     "Adquisicion de Material de Campo para Encuestas",           "BIEN",     "MATERIAL CAMPO",   "PAGADO",        3300.00,  3250.00,  3),
    ("DEC",      "Adquisicion de Toner y Cartuchos para DEC",                 "BIEN",     "SUMINISTROS TI",   "PAGADO",        2800.00,  2750.00,  3),
    ("DTI",      "Servicio de Mantenimiento de Vehiculos DTI",                "SERVICIO", "MANTENIMIENTO",    "ORDEN_EMITIDA", 7500.00,  None,     2),
    ("OTA",      "Adquisicion de Uniformes para Personal OTA",                "BIEN",     "VESTUARIO",        "PENDIENTE",     6500.00,  None,     0),
]

# ---------------------------------------------------------------------------
# Actividades Operativas 2025 (30 AOs)
# Tuplas: (ue_sigla, codigo_ceplan, nombre, oei, aei, ejec_pct)
# ---------------------------------------------------------------------------

AO_DATA: list[tuple[str, str, str, str, str, float]] = [
    ("OTIN",     "AOI00000500001", "Gestion de Infraestructura Tecnologica INEI",
     "Modernizar los sistemas de informacion",
     "Implementar plataformas de TI seguras",
     98.5),
    ("OTIN",     "AOI00000500002", "Desarrollo y Mantenimiento de Sistemas Web",
     "Fortalecer la gestion de datos estadisticos",
     "Desarrollar aplicaciones para gestion interna",
     95.2),
    ("OTIN",     "AOI00000500003", "Seguridad Informatica y Ciberseguridad",
     "Modernizar los sistemas de informacion",
     "Garantizar seguridad de datos institucionales",
     91.8),
    ("DEC",      "AOI00000500004", "Produccion de Indicadores Economicos Mensuales",
     "Generar estadisticas de calidad y oportunidad",
     "Elaborar indices de precios y empleo",
     97.3),
    ("DEC",      "AOI00000500005", "Encuesta Nacional de Hogares 2025",
     "Generar estadisticas sociales y demograficas",
     "Ejecutar encuestas nacionales representativas",
     94.6),
    ("DEC",      "AOI00000500006", "Indicadores de Pobreza y Desarrollo Social",
     "Medir condiciones de vida de la poblacion",
     "Calcular indices multidimensionales de pobreza",
     88.9),
    ("OTA",      "AOI00000500007", "Gestion Administrativa y Logistica Institucional",
     "Optimizar procesos administrativos",
     "Mejorar eficiencia en adquisiciones y contratos",
     92.1),
    ("OTA",      "AOI00000500008", "Capacitacion y Desarrollo del Personal INEI",
     "Fortalecer capacidades del recurso humano",
     "Ejecutar programa anual de capacitaciones",
     87.4),
    ("OTPP",     "AOI00000500009", "Formulacion y Seguimiento del Plan Operativo",
     "Asegurar la planificacion institucional",
     "Monitorear cumplimiento de metas y objetivos",
     90.5),
    ("OTPP",     "AOI00000500010", "Presupuesto por Resultados y Control Presupuestal",
     "Optimizar ejecucion presupuestaria",
     "Garantizar transparencia en uso de recursos",
     85.7),
    ("DNCE",     "AOI00000500011", "Publicacion de Anuario Estadistico del Peru",
     "Difundir estadisticas oficiales del pais",
     "Publicar compendio estadistico nacional",
     93.4),
    ("DNCPP",    "AOI00000500012", "Preparacion Censal y Actualizacion Cartografica",
     "Actualizar informacion censal y territorial",
     "Preparar infraestructura para censos futuros",
     89.2),
    ("DNEL",     "AOI00000500013", "Produccion de Estadisticas de Empleo y Remuneraciones",
     "Medir mercado laboral nacional",
     "Elaborar encuestas de empresas y trabajadores",
     84.3),
    ("DTI",      "AOI00000500014", "Mantenimiento de Infraestructura Fisica Institucional",
     "Conservar activos institucionales",
     "Ejecutar mantenimiento preventivo y correctivo",
     91.6),
    ("OTIN",     "AOI00000500015", "Modernizacion de Sistemas de Informacion Estadistica",
     "Implementar nuevas plataformas digitales",
     "Desarrollar sistemas de gestion de datos",
     95.3),
    ("DEC",      "AOI00000500016", "Encuesta de Condiciones de Vida y Pobreza 2025",
     "Medir niveles de bienestar de la poblacion",
     "Ejecutar encuesta ENAHO a nivel nacional",
     91.8),
    ("OTA",      "AOI00000500017", "Gestion de Recursos Humanos y Bienestar del Personal",
     "Optimizar gestion del capital humano",
     "Implementar sistema de evaluacion de desempeno",
     87.4),
    ("OTPP",     "AOI00000500018", "Planificacion Estrategica y Presupuestal 2025-2027",
     "Alinear objetivos institucionales con recursos",
     "Elaborar Plan Estrategico Institucional",
     92.6),
    ("DNCE",     "AOI00000500019", "Produccion de Estadisticas Economicas Sectoriales",
     "Generar estadisticas del sector empresarial",
     "Elaborar informes de coyuntura economica",
     89.1),
    ("DNCPP",    "AOI00000500020", "Actualizacion del Marco Cartografico Nacional",
     "Mantener informacion geografica actualizada",
     "Actualizar base cartografica para censos",
     86.3),
    ("DNEL",     "AOI00000500021", "Encuesta Nacional de Empresas 2025",
     "Medir estructura empresarial del pais",
     "Levantar datos de unidades economicas",
     88.7),
    ("DTI",      "AOI00000500022", "Implementacion de Ciberseguridad Institucional",
     "Proteger activos de informacion institucionales",
     "Implementar controles de seguridad informatica",
     94.5),
    ("OTIN",     "AOI00000500023", "Desarrollo de la Plataforma de Datos Abiertos INEI",
     "Democratizar el acceso a datos estadisticos",
     "Publicar datasets en portal de datos abiertos",
     90.2),
    ("DEC",      "AOI00000500024", "Encuesta Nacional de Hogares Rurales 2025",
     "Medir condiciones en zonas rurales",
     "Ejecutar ENAHO modulo rural",
     83.6),
    ("OTA",      "AOI00000500025", "Gestion Documental y Archivo Institucional",
     "Preservar memoria institucional",
     "Digitalizar y organizar archivos historicos",
     91.0),
    ("DNCE",     "AOI00000500026", "Publicacion Digital de Estadisticas Oficiales",
     "Difundir estadisticas por medios digitales",
     "Actualizar portal web de estadisticas",
     93.8),
    ("DNCPP",    "AOI00000500027", "Capacitacion de Encuestadores para Censos 2025",
     "Fortalecer capacidades del personal de campo",
     "Ejecutar programa de formacion censal",
     87.2),
    ("DNEL",     "AOI00000500028", "Estadisticas de Remuneraciones y Condiciones de Trabajo",
     "Medir ingresos laborales por sector",
     "Elaborar indices de remuneraciones",
     85.4),
    ("DTI",      "AOI00000500029", "Mantenimiento y Soporte de Sistemas Administrativos",
     "Garantizar continuidad operativa de sistemas",
     "Ejecutar mantenimiento preventivo de plataformas",
     96.1),
    ("OTPP",     "AOI00000500030", "Evaluacion de Resultados Presupuestales 2025",
     "Medir eficacia del gasto publico institucional",
     "Analizar indicadores de desempeno presupuestal",
     88.9),
]


# ---------------------------------------------------------------------------
# Logica de hitos — Adquisicion
# ---------------------------------------------------------------------------


def _estado_hito_adq(orden: int, estado_adq: str) -> str:
    """Determina el estado de un hito segun el estado general de la adquisicion."""
    if estado_adq == "CULMINADO":
        return "COMPLETADO"

    if estado_adq == "ADJUDICADO":
        # Hitos 1-18 COMPLETADO, 19-22 segun posicion
        if orden <= 18:
            return "COMPLETADO"
        elif orden == 19:
            return "EN_CURSO"
        else:
            return "PENDIENTE"

    if estado_adq == "EN_SELECCION":
        if orden <= 14:
            return "COMPLETADO"
        elif orden <= 17:
            return "EN_CURSO"
        else:
            return "PENDIENTE"

    if estado_adq == "EN_ACTOS_PREPARATORIOS":
        if orden <= 5:
            return "COMPLETADO"
        elif orden <= 9:
            return "EN_CURSO"
        else:
            return "PENDIENTE"

    if estado_adq == "DESIERTO":
        if orden <= 16:
            return "COMPLETADO"
        elif orden == 17:
            return "OBSERVADO"
        else:
            return "PENDIENTE"

    # EN_EJECUCION, NULO y otros: tratar como ADJUDICADO
    if orden <= 18:
        return "COMPLETADO"
    elif orden == 19:
        return "EN_CURSO"
    return "PENDIENTE"


def _fechas_hito_adq(
    hitos_def: list[tuple[int, str, str, str, int]],
    orden: int,
    estado_adq: str,
    estado_hito: str,
) -> tuple[date | None, date | None, date | None, date | None]:
    """Calcula fechas planificadas y reales de un hito de adquisicion.

    Returns:
        (fecha_inicio, fecha_fin, fecha_real_inicio, fecha_real_fin)
    """
    base = ESTADO_FECHA_BASE.get(estado_adq, _d(2025, 1, 5))
    # Acumular dias hasta el hito anterior
    offset_dias = 0
    for h in hitos_def:
        if h[0] < orden:
            offset_dias += int(h[4] * 1.2)

    fi = base + timedelta(days=offset_dias)
    ff = fi + timedelta(days=int(hitos_def[orden - 1][4] * 1.2))

    if estado_hito == "COMPLETADO":
        return fi, ff, fi, ff
    elif estado_hito == "EN_CURSO":
        return fi, ff, fi, None
    elif estado_hito == "OBSERVADO":
        return fi, ff, fi, None
    else:  # PENDIENTE
        return fi, ff, None, None


# ---------------------------------------------------------------------------
# Logica de hitos — Contrato Menor
# ---------------------------------------------------------------------------


def _estado_hito_cm(orden: int, estado_cm: str) -> str:
    """Determina el estado de un hito segun el estado del contrato menor."""
    if estado_cm == "PAGADO":
        return "COMPLETADO"

    if estado_cm == "EJECUTADO":
        # hitos 1-8 COMPLETADO, 9 EN_CURSO
        if orden <= 8:
            return "COMPLETADO"
        return "EN_CURSO"

    if estado_cm == "ORDEN_EMITIDA":
        if orden <= 7:
            return "COMPLETADO"
        return "PENDIENTE"

    if estado_cm == "EN_PROCESO":
        if orden <= 5:
            return "COMPLETADO"
        return "PENDIENTE"

    # PENDIENTE
    if orden <= 2:
        return "COMPLETADO"
    return "PENDIENTE"


def _fechas_hito_cm(
    hitos_def: list[tuple[int, str, str, int]],
    orden: int,
    base: date,
    estado_hito: str,
) -> tuple[date | None, date | None]:
    """Calcula fechas planificadas de un hito de contrato menor.

    Returns:
        (fecha_inicio, fecha_fin)
    """
    offset_dias = 0
    for h in hitos_def:
        if h[0] < orden:
            offset_dias += h[3]

    fi = base + timedelta(days=offset_dias)
    ff = fi + timedelta(days=hitos_def[orden - 1][3])
    return fi, ff


# ---------------------------------------------------------------------------
# Asignacion de proveedor segun tipo de bien/servicio
# ---------------------------------------------------------------------------

# Indices en PROVEEDORES_DATOS (despues de crear/cargar):
# 0  IBM Peru         → BIEN TI
# 1  Microsoft Peru   → BIEN software
# 2  Papeleria        → BIEN material
# 3  STA Soluciones   → SERVICIO TI
# 4  CEP Consultores  → SERVICIO consultoria
# 5  Electro J&M      → SERVICIO mantenimiento
# 6  Lindley          → BIEN consumo / SERVICIO catering
# 7  Fumigacion Peru  → SERVICIO limpieza
# 8  SVA Seguridad    → SERVICIO vigilancia
# 9  Telefonica       → SERVICIO comunicaciones
# 10 Claro Peru       → SERVICIO comunicaciones (alternativo)
# 11 Lenovo Peru      → BIEN TI hardware alternativo
# 12 Impresiones Peru → BIEN/SERVICIO impresion
# 13 DevSoft Peru     → SERVICIO software
# 14 Capacita Peru    → SERVICIO capacitacion

_PROVEEDOR_TI_BIEN     = [0, 11, 3]   # IBM, Lenovo, STA
_PROVEEDOR_TI_SERV     = [3, 13, 0]   # STA, DevSoft, IBM
_PROVEEDOR_MATERIAL    = [2, 6, 2]    # Papeleria, Lindley, Papeleria
_PROVEEDOR_CONSULTORIA = [4, 14, 4]   # CEP, Capacita, CEP
_PROVEEDOR_LIMPIEZA    = [7, 8, 7]    # Fumigacion, SVA, Fumigacion
_PROVEEDOR_COMUN       = [2, 5, 12]   # Papeleria, Electro, Impresiones


def _pick_proveedor_idx(
    tipo_objeto: str, descripcion: str, seq: int
) -> int:
    """Selecciona indice de proveedor segun tipo y descripcion."""
    desc = descripcion.lower()
    idx = seq % 3

    if tipo_objeto == "BIEN":
        if any(k in desc for k in ("servidor", "computo", "tablet", "equipo", "software", "ssd", "ram", "mouse", "switch", "cable", "memoria", "disco")):
            return _PROVEEDOR_TI_BIEN[idx]
        if any(k in desc for k in ("papel", "lapicero", "utile", "archivador", "folder", "agenda", "toner", "cartucho", "usb", "medio")):
            return _PROVEEDOR_MATERIAL[idx]
        if any(k in desc for k in ("impresion", "formulario", "encuesta", "tablero", "clipboard")):
            return 12  # Impresiones Peru
        if any(k in desc for k in ("mobiliario", "oficina", "equipo de oficina")):
            return 5   # Electro J&M
        if any(k in desc for k in ("combustible", "carburante")):
            return 6   # Lindley (distribuidor)
        if any(k in desc for k in ("chaleco", "gorra", "mochila", "vestuario", "uniforme", "prendas")):
            return 2   # Papeleria La Union (bienes generales)
        if any(k in desc for k in ("vehiculo", "auto", "camioneta")):
            return 5   # Electro J&M
        if any(k in desc for k in ("libro", "publicacion", "anuario")):
            return 12  # Impresiones Peru
        if any(k in desc for k in ("medicion",)):
            return 0   # IBM Peru
        return _PROVEEDOR_MATERIAL[idx]

    # SERVICIO
    if any(k in desc for k in ("soporte", "configuracion", "mantenimiento de equipo", "red", "lan", "wan", "ti", "sistema", "software", "aplicacion", "informatica", "ciberseguridad")):
        return _PROVEEDOR_TI_SERV[idx]
    if any(k in desc for k in ("consultoria", "levantamiento", "modernizacion", "estadistica")):
        return _PROVEEDOR_CONSULTORIA[idx]
    if any(k in desc for k in ("limpieza", "fumigacion", "lavanderia")):
        return 7  # Fumigacion Peru
    if any(k in desc for k in ("seguridad", "vigilancia")):
        return 8  # SVA Seguridad
    if any(k in desc for k in ("capacitacion", "entrenamiento")):
        return 14  # Capacita Peru
    if any(k in desc for k in ("impresion", "encuadernacion", "publicacion", "fotocopiado")):
        return 12  # Impresiones Peru
    if any(k in desc for k in ("transporte", "mensajeria", "courier", "movilidad")):
        return 6   # Lindley (logistica)
    if any(k in desc for k in ("comunicacion", "telefonia", "internet")):
        return 9 if idx == 0 else 10  # Telefonica o Claro
    if any(k in desc for k in ("catering", "alimento", "bebida")):
        return 6   # Lindley
    if any(k in desc for k in ("traduccion",)):
        return 4   # CEP Consultores
    if any(k in desc for k in ("mantenimiento",)):
        return 5   # Electro J&M
    if any(k in desc for k in ("encuestador", "empadronador")):
        return 4   # CEP Consultores
    return _PROVEEDOR_COMUN[idx]


# ---------------------------------------------------------------------------
# Fechas base para contratos menores segun estado y posicion en lista
# ---------------------------------------------------------------------------

def _base_fecha_cm(estado: str, seq: int) -> date:
    """Fecha de inicio del primer hito segun estado y posicion."""
    if estado in ("PAGADO", "EJECUTADO"):
        # Distribuir entre enero y octubre segun secuencia
        mes = 1 + (seq % 10)
        return _d(2025, mes, 5)
    elif estado == "ORDEN_EMITIDA":
        return _d(2025, 10, 10)
    elif estado == "EN_PROCESO":
        return _d(2025, 11, 1)
    else:  # PENDIENTE
        return _d(2025, 11, 20)


# ---------------------------------------------------------------------------
# Funcion principal de sembrado
# ---------------------------------------------------------------------------


def sembrar_2025_completo(force: bool = False) -> None:
    """Siembra todos los datos 2025 en la base de datos.

    Args:
        force: Si True, elimina datos 2025 existentes y re-siembra.
    """
    session = SessionLocal()

    try:
        # ------------------------------------------------------------------
        # GUARDIA DE IDEMPOTENCIA
        # ------------------------------------------------------------------
        existentes = (
            session.query(ProgramacionPresupuestal)
            .filter(ProgramacionPresupuestal.anio == ANIO)
            .count()
        )

        if existentes > 0 and not force:
            print(
                f"[SKIP] Ya existen {existentes} registros ProgramacionPresupuestal "
                f"para el anio {ANIO}. Ejecutar con --force para resetear."
            )
            return

        if existentes > 0 and force:
            print(f"[FORCE] Eliminando datos {ANIO} existentes...")
            # Eliminar en orden inverso de dependencias
            # Adquisiciones
            adq_ids = [
                r[0] for r in session.query(Adquisicion.id)
                .filter(Adquisicion.anio == ANIO).all()
            ]
            if adq_ids:
                session.query(AdquisicionProceso).filter(
                    AdquisicionProceso.adquisicion_id.in_(adq_ids)
                ).delete(synchronize_session=False)
                session.query(AdquisicionDetalle).filter(
                    AdquisicionDetalle.adquisicion_id.in_(adq_ids)
                ).delete(synchronize_session=False)
                session.query(Adquisicion).filter(
                    Adquisicion.id.in_(adq_ids)
                ).delete(synchronize_session=False)

            # Contratos menores
            cm_ids = [
                r[0] for r in session.query(ContratoMenor.id)
                .filter(ContratoMenor.anio == ANIO).all()
            ]
            if cm_ids:
                session.query(ContratoMenorProceso).filter(
                    ContratoMenorProceso.contrato_menor_id.in_(cm_ids)
                ).delete(synchronize_session=False)
                session.query(ContratoMenor).filter(
                    ContratoMenor.id.in_(cm_ids)
                ).delete(synchronize_session=False)

            # AOs
            session.query(ActividadOperativa).filter(
                ActividadOperativa.anio == ANIO
            ).delete(synchronize_session=False)

            # Presupuesto
            pp_ids = [
                r[0] for r in session.query(ProgramacionPresupuestal.id)
                .filter(ProgramacionPresupuestal.anio == ANIO).all()
            ]
            if pp_ids:
                session.query(ProgramacionMensual).filter(
                    ProgramacionMensual.programacion_presupuestal_id.in_(pp_ids)
                ).delete(synchronize_session=False)
                session.query(ProgramacionPresupuestal).filter(
                    ProgramacionPresupuestal.id.in_(pp_ids)
                ).delete(synchronize_session=False)

            # Metas
            session.query(MetaPresupuestal).filter(
                MetaPresupuestal.anio == ANIO
            ).delete(synchronize_session=False)

            session.flush()
            print("    Datos eliminados correctamente.")

        # ------------------------------------------------------------------
        # PASO 1 — UEs y Clasificadores
        # ------------------------------------------------------------------
        print(f"[1/6] Cargando UEs y clasificadores...")

        ues: dict[str, UnidadEjecutora] = {
            ue.sigla: ue for ue in session.query(UnidadEjecutora).all()
        }
        print(f"    UEs encontradas en BD: {len(ues)}")

        # Clasificadores
        clasifs: dict[str, ClasificadorGasto] = {
            c.codigo: c for c in session.query(ClasificadorGasto).all()
        }
        clasifs_creados = 0
        for codigo, descripcion, tipo_generico in CLASIFS_DATOS:
            if codigo not in clasifs:
                nc = ClasificadorGasto(
                    codigo=codigo,
                    descripcion=descripcion,
                    tipo_generico=tipo_generico,
                )
                session.add(nc)
                clasifs[codigo] = nc
                clasifs_creados += 1

        if clasifs_creados > 0:
            session.flush()
        print(f"    Clasificadores creados: {clasifs_creados}")

        # Metas (1 por UE para el anio)
        metas_existentes: dict[int, MetaPresupuestal] = {
            m.ue_id: m
            for m in session.query(MetaPresupuestal)
            .filter(MetaPresupuestal.anio == ANIO)
            .all()
        }
        metas: dict[str, MetaPresupuestal] = {}
        metas_creadas = 0

        for sigla, ue in ues.items():
            if ue.id is None:
                session.flush()
            if ue.id in metas_existentes:
                metas[sigla] = metas_existentes[ue.id]
            else:
                codigo_meta = f"25{ue.id:04d}"  # max 6 chars <= String(10)
                nueva_meta = MetaPresupuestal(
                    codigo=codigo_meta,
                    descripcion=f"Meta Presupuestal {ANIO} - {ue.nombre}",
                    sec_funcional=None,
                    ue_id=ue.id,
                    anio=ANIO,
                    activo=True,
                )
                session.add(nueva_meta)
                metas[sigla] = nueva_meta
                metas_creadas += 1

        if metas_creadas > 0:
            session.flush()
        print(f"    Metas creadas: {metas_creadas}")

        # ------------------------------------------------------------------
        # PASO 2 — Presupuesto (ProgramacionPresupuestal + ProgramacionMensual)
        # ------------------------------------------------------------------
        print(f"[2/6] Generando programaciones presupuestales {ANIO}...")
        total_pp = 0
        total_pm = 0

        for indice_ue, sigla in enumerate(sorted(UE_BUDGET_2025.keys())):
            ue = ues.get(sigla)
            if ue is None:
                print(f"    [WARN] UE '{sigla}' no encontrada. Se omite.")
                continue
            meta = metas.get(sigla)
            if meta is None:
                print(f"    [WARN] Meta '{sigla}' no encontrada. Se omite.")
                continue

            pim_ue = UE_BUDGET_2025[sigla]
            pct_ejec = UE_EJECUCION_PCT.get(sigla, 85.0)

            clasifs_ue = UE_CLASIFS_MAP.get(sigla, CLASIFS_ESTADISTICA)

            for indice_clasif, (cod_clasif, pct_clasif) in enumerate(clasifs_ue):
                clasif = clasifs.get(cod_clasif)
                if clasif is None:
                    continue

                montos = _calcular_montos(pim_ue, pct_ejec, pct_clasif)
                fuente = _fuente_financiamiento(sigla, indice_clasif)

                pp = ProgramacionPresupuestal(
                    anio=ANIO,
                    ue_id=ue.id,
                    meta_id=meta.id,
                    clasificador_id=clasif.id,
                    pia=montos["pia"],
                    pim=montos["pim"],
                    certificado=montos["certificado"],
                    compromiso_anual=montos["compromiso_anual"],
                    devengado=montos["devengado"],
                    girado=montos["girado"],
                    saldo=montos["saldo"],
                    fuente_financiamiento=fuente,
                )
                session.add(pp)
                session.flush()

                meses = _calcular_meses(montos["pim"], montos["devengado"])
                for mes_num, (programado, ejecutado, saldo) in enumerate(meses, start=1):
                    session.add(ProgramacionMensual(
                        programacion_presupuestal_id=pp.id,
                        mes=mes_num,
                        programado=programado,
                        ejecutado=ejecutado,
                        saldo=saldo,
                    ))
                    total_pm += 1
                total_pp += 1

        print(f"    ProgramacionPresupuestal: {total_pp} | ProgramacionMensual: {total_pm}")

        # ------------------------------------------------------------------
        # PASO 3 — Proveedores
        # ------------------------------------------------------------------
        print(f"[3/6] Cargando proveedores...")

        provs_existentes: dict[str, Proveedor] = {
            p.ruc: p for p in session.query(Proveedor).all()
        }
        proveedores_list: list[Proveedor] = []
        provs_creados = 0

        for ruc, razon_social, nombre_comercial in PROVEEDORES_DATOS:
            if ruc in provs_existentes:
                proveedores_list.append(provs_existentes[ruc])
            else:
                np_ = Proveedor(
                    ruc=ruc,
                    razon_social=razon_social,
                    nombre_comercial=nombre_comercial,
                    estado_rnp="HABIDO",
                    activo=True,
                )
                session.add(np_)
                proveedores_list.append(np_)
                provs_creados += 1

        if provs_creados > 0:
            session.flush()
        print(f"    Proveedores creados: {provs_creados} | Total disponibles: {len(proveedores_list)}")

        # ------------------------------------------------------------------
        # PASO 4 — Adquisiciones 2025 (22 registros + hitos)
        # ------------------------------------------------------------------
        print(f"[4/6] Creando adquisiciones 2025...")
        total_hitos_adq = 0

        for seq, ue_sigla, descripcion, tipo_objeto, tipo_proc, estado, monto_ref in ADQ_DATA:
            ue = ues.get(ue_sigla)
            if ue is None:
                print(f"    [WARN] UE '{ue_sigla}' no encontrada para ADQ-2025-{seq:03d}.")
                continue
            meta = metas.get(ue_sigla)

            fase_actual = ESTADO_FASE_MAP.get(estado, "ACTUACIONES_PREPARATORIAS")

            # Monto adjudicado solo para estados con contrato firmado
            if estado in ("CULMINADO", "ADJUDICADO", "EN_EJECUCION"):
                # Factor pseudo-aleatorio determinista entre 0.94 y 0.98
                factor = 0.94 + (seq % 5) * 0.01
                monto_adj: float | None = round(monto_ref * factor, 2)
            else:
                monto_adj = None

            # Proveedor
            prov_idx = _pick_proveedor_idx(tipo_objeto, descripcion, seq)
            proveedor = proveedores_list[prov_idx] if prov_idx < len(proveedores_list) else proveedores_list[0]

            # Solo asignar proveedor si ya fue adjudicado
            prov_id = proveedor.id if estado in ("CULMINADO", "ADJUDICADO", "EN_EJECUCION") else None

            adq = Adquisicion(
                codigo=f"ADQ-2025-{seq:03d}",
                anio=ANIO,
                ue_id=ue.id,
                meta_id=meta.id if meta else None,
                descripcion=descripcion,
                tipo_objeto=tipo_objeto,
                tipo_procedimiento=tipo_proc,
                estado=estado,
                fase_actual=fase_actual,
                monto_referencial=_dec(monto_ref),
                monto_adjudicado=_dec(monto_adj) if monto_adj else None,
                proveedor_id=prov_id,
            )
            session.add(adq)
            session.flush()

            # AdquisicionDetalle (1:1, campos minimos)
            det = AdquisicionDetalle(
                adquisicion_id=adq.id,
                n_expediente=f"EXP-2025-{seq:04d}",
                n_proceso_seace=f"SEACE-2025-{seq:05d}" if estado not in ("EN_ACTOS_PREPARATORIOS",) else None,
                observaciones=None,
            )
            session.add(det)

            # Hitos del proceso (22 hitos)
            for orden, hito, fase, area_resp, dias_plan in HITOS_ADQ:
                est_hito = _estado_hito_adq(orden, estado)
                fi, ff, fri, frf = _fechas_hito_adq(HITOS_ADQ, orden, estado, est_hito)

                session.add(AdquisicionProceso(
                    adquisicion_id=adq.id,
                    orden=orden,
                    hito=hito,
                    fase=fase,
                    area_responsable=area_resp,
                    dias_planificados=dias_plan,
                    fecha_inicio=fi,
                    fecha_fin=ff,
                    fecha_real_inicio=fri,
                    fecha_real_fin=frf,
                    estado=est_hito,
                    observacion="Proceso declarado desierto" if est_hito == "OBSERVADO" else None,
                ))
                total_hitos_adq += 1

        session.flush()
        print(f"    Adquisiciones: 22 | Hitos ADQ: {total_hitos_adq}")

        # ------------------------------------------------------------------
        # PASO 5 — Contratos Menores 2025 (40 registros + hitos)
        # ------------------------------------------------------------------
        print(f"[5/6] Creando contratos menores 2025...")
        total_hitos_cm = 0

        for seq_0, (ue_sigla, descripcion, tipo_obj, categoria, estado_cm, monto_est, monto_ejec, n_cotiz) in enumerate(CM_DATA):
            seq = seq_0 + 1
            ue = ues.get(ue_sigla)
            if ue is None:
                print(f"    [WARN] UE '{ue_sigla}' no encontrada para CM-2025-{seq:03d}.")
                continue
            meta = metas.get(ue_sigla)

            # Proveedor para CM (solo si tiene monto ejecutado)
            prov_id_cm: int | None = None
            if monto_ejec is not None:
                prov_idx_cm = _pick_proveedor_idx(tipo_obj, descripcion, seq)
                prov_cm = proveedores_list[prov_idx_cm] if prov_idx_cm < len(proveedores_list) else proveedores_list[0]
                prov_id_cm = prov_cm.id

            # n_orden solo si ORDEN_EMITIDA, EJECUTADO o PAGADO
            n_orden_val: str | None = None
            if estado_cm in ("ORDEN_EMITIDA", "EJECUTADO", "PAGADO"):
                n_orden_val = f"OC-2025-{seq:04d}" if tipo_obj == "BIEN" else f"OS-2025-{seq:04d}"

            cm = ContratoMenor(
                codigo=f"CM-2025-{seq:03d}",
                anio=ANIO,
                ue_id=ue.id,
                meta_id=meta.id if meta else None,
                descripcion=descripcion,
                tipo_objeto=tipo_obj,
                categoria=categoria,
                estado=estado_cm,
                monto_estimado=_dec(monto_est),
                monto_ejecutado=_dec(monto_ejec) if monto_ejec is not None else None,
                proveedor_id=prov_id_cm,
                n_orden=n_orden_val,
                n_cotizaciones=n_cotiz,
            )
            session.add(cm)
            session.flush()

            # Hitos del contrato menor (9 pasos)
            base_cm = _base_fecha_cm(estado_cm, seq_0)
            for orden_cm, hito_cm, area_cm, dias_cm in HITOS_CM:
                est_hito_cm = _estado_hito_cm(orden_cm, estado_cm)
                fi_cm, ff_cm = _fechas_hito_cm(HITOS_CM, orden_cm, base_cm, est_hito_cm)

                session.add(ContratoMenorProceso(
                    contrato_menor_id=cm.id,
                    orden=orden_cm,
                    hito=hito_cm,
                    area_responsable=area_cm,
                    dias_planificados=dias_cm,
                    fecha_inicio=fi_cm,
                    fecha_fin=ff_cm,
                    estado=est_hito_cm,
                ))
                total_hitos_cm += 1

        session.flush()
        print(f"    Contratos Menores: 40 | Hitos CM: {total_hitos_cm}")

        # ------------------------------------------------------------------
        # PASO 6 — Actividades Operativas 2025 (30 AOs)
        # ------------------------------------------------------------------
        print(f"[6/6] Creando actividades operativas 2025...")
        total_aos = 0

        for ue_sigla, cod_ceplan, nombre, oei, aei, ejec_pct in AO_DATA:
            ue = ues.get(ue_sigla)
            if ue is None:
                print(f"    [WARN] UE '{ue_sigla}' no encontrada para AO {cod_ceplan}.")
                continue
            meta = metas.get(ue_sigla)

            ao = ActividadOperativa(
                codigo_ceplan=cod_ceplan,
                nombre=nombre,
                oei=oei,
                aei=aei,
                meta_id=meta.id if meta else None,
                ue_id=ue.id,
                anio=ANIO,
                activo=True,
            )
            session.add(ao)
            total_aos += 1

        session.flush()
        print(f"    Actividades Operativas: {total_aos}")

        # ------------------------------------------------------------------
        # COMMIT FINAL
        # ------------------------------------------------------------------
        session.commit()

        print()
        print("=" * 65)
        print(f"  SEED 2025 COMPLETO — OK")
        print("=" * 65)
        print(f"  ProgramacionPresupuestal : {total_pp}")
        print(f"  ProgramacionMensual      : {total_pm}")
        print(f"  Adquisiciones            : 22 | Hitos ADQ : {total_hitos_adq}")
        print(f"  Contratos Menores        : 40 | Hitos CM  : {total_hitos_cm}")
        print(f"  Actividades Operativas   : {total_aos}")
        print("=" * 65)

    except Exception as exc:
        session.rollback()
        print(f"\n[ERROR] Transaccion revertida. Detalle: {exc}")
        raise

    finally:
        session.close()


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    force_flag = "--force" in sys.argv
    sembrar_2025_completo(force=force_flag)
