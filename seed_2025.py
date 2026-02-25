"""Sembrado de datos realistas del año 2025 para el Dashboard INEI.

Siembra los siguientes registros:
- 25 ODEIs regionales (si no existen)
- 18 ClasificadorGasto (si no existen)
- 1 MetaPresupuestal por UE para el año 2025
- N ProgramacionPresupuestal (UE × clasificadores asignados)
- 12 ProgramacionMensual por cada ProgramacionPresupuestal

El script es idempotente: si ya existen datos 2025, termina sin modificar nada.

Ejecución:
    cd backend
    python seed_2025.py
"""

import sys
import os
from decimal import Decimal, ROUND_HALF_UP

# Asegura que el directorio raíz del backend esté en el path de Python
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal
from app.models import (
    UnidadEjecutora,
    MetaPresupuestal,
    ClasificadorGasto,
    ProgramacionPresupuestal,
    ProgramacionMensual,
)

# ---------------------------------------------------------------------------
# Constantes globales
# ---------------------------------------------------------------------------

ANIO = 2025

# Pesos mensuales de ejecución (suma = 1.0).
# Refleja el patrón típico del gobierno peruano: inicio lento, aceleración
# desde marzo, plateau en el semestre 2, cierre en diciembre.
MES_PESOS = [0.04, 0.06, 0.08, 0.09, 0.10, 0.10, 0.10, 0.10, 0.09, 0.10, 0.08, 0.06]

# Verificación interna: los pesos deben sumar exactamente 1.0
assert abs(sum(MES_PESOS) - 1.0) < 1e-9, "MES_PESOS no suma 1.0"

# ---------------------------------------------------------------------------
# Datos maestros de UEs — ODEIs a crear si no existen
# Tuplas: (codigo, nombre, sigla, tipo)
# ---------------------------------------------------------------------------

ODEIS_DATOS = [
    ("O-AMA", "ODEI Amazonas",      "ODEI-AMA", "ODEI"),
    ("O-ANC", "ODEI Ancash",        "ODEI-ANC", "ODEI"),
    ("O-APU", "ODEI Apurimac",      "ODEI-APU", "ODEI"),
    ("O-ARE", "ODEI Arequipa",      "ODEI-ARE", "ODEI"),
    ("O-AYA", "ODEI Ayacucho",      "ODEI-AYA", "ODEI"),
    ("O-CAJ", "ODEI Cajamarca",     "ODEI-CAJ", "ODEI"),
    ("O-CAL", "ODEI Callao",        "ODEI-CAL", "ODEI"),
    ("O-CUS", "ODEI Cusco",         "ODEI-CUS", "ODEI"),
    ("O-HVC", "ODEI Huancavelica",  "ODEI-HVC", "ODEI"),
    ("O-HUC", "ODEI Huanuco",       "ODEI-HUC", "ODEI"),
    ("O-ICA", "ODEI Ica",           "ODEI-ICA", "ODEI"),
    ("O-JUN", "ODEI Junin",         "ODEI-JUN", "ODEI"),
    ("O-LAL", "ODEI La Libertad",   "ODEI-LAL", "ODEI"),
    ("O-LAM", "ODEI Lambayeque",    "ODEI-LAM", "ODEI"),
    ("O-LIM", "ODEI Lima",          "ODEI-LIM", "ODEI"),
    ("O-LOR", "ODEI Loreto",        "ODEI-LOR", "ODEI"),
    ("O-MAD", "ODEI Madre de Dios", "ODEI-MAD", "ODEI"),
    ("O-MOQ", "ODEI Moquegua",      "ODEI-MOQ", "ODEI"),
    ("O-PAS", "ODEI Pasco",         "ODEI-PAS", "ODEI"),
    ("O-PIU", "ODEI Piura",         "ODEI-PIU", "ODEI"),
    ("O-PUN", "ODEI Puno",          "ODEI-PUN", "ODEI"),
    ("O-SAM", "ODEI San Martin",    "ODEI-SAM", "ODEI"),
    ("O-TAC", "ODEI Tacna",         "ODEI-TAC", "ODEI"),
    ("O-TUM", "ODEI Tumbes",        "ODEI-TUM", "ODEI"),
    ("O-UCA", "ODEI Ucayali",       "ODEI-UCA", "ODEI"),
]

# ---------------------------------------------------------------------------
# Clasificadores de gasto a crear si no existen
# Tuplas: (codigo, descripcion, tipo_generico)
# tipo_generico es el prefijo de nivel 1 del código ("2.3", "2.6", etc.)
# ---------------------------------------------------------------------------

CLASIFS_DATOS = [
    ("2.3.1.5.1.2",   "Papeleria y utiles de oficina",         "2.3"),
    ("2.3.2.2.2.3",   "Equipos de computo y accesorios",       "2.3"),
    ("2.3.2.7.11.99", "Otros bienes de tecnologia",            "2.3"),
    ("2.3.1.99.1.99", "Otros bienes de consumo",               "2.3"),
    ("2.3.2.7.2.99",  "Software y licencias informaticas",     "2.3"),
    ("2.3.2.4.1.1",   "Mantenimiento de equipos de computo",   "2.3"),
    ("2.3.2.8.1.1",   "Servicios de comunicaciones",           "2.3"),
    ("2.3.1.2.1.1",   "Vestuario uniformes y prendas",         "2.3"),
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
]

# ---------------------------------------------------------------------------
# PIM 2025 por sigla de UE (en soles)
# ---------------------------------------------------------------------------

UE_BUDGET_2025: dict[str, int] = {
    # Oficinas centrales
    "OTIN":     18_200_000,
    "DEC":      22_500_000,
    "OTA":       8_100_000,
    "OTPP":      6_300_000,
    "DNCPP":     4_800_000,
    "DNCE":      5_200_000,
    "DNEL":      3_900_000,
    "DTI":       4_100_000,
    # ODEIs grandes
    "ODEI-ARE":  5_200_000,
    "ODEI-LAL":  4_800_000,
    "ODEI-PIU":  4_500_000,
    "ODEI-CUS":  4_200_000,
    "ODEI-JUN":  4_000_000,
    # ODEIs medianos
    "ODEI-LAM":  3_800_000,
    "ODEI-CAJ":  3_500_000,
    "ODEI-LIM":  3_500_000,
    "ODEI-ANC":  3_300_000,
    "ODEI-PUN":  3_200_000,
    "ODEI-ICA":  3_100_000,
    "ODEI-SAM":  3_000_000,
    "ODEI-AYA":  2_900_000,
    # ODEIs pequeños
    "ODEI-LOR":  2_800_000,
    "ODEI-UCA":  2_200_000,
    "ODEI-CAL":  2_300_000,
    "ODEI-HUC":  2_100_000,
    "ODEI-TAC":  1_800_000,
    "ODEI-AMA":  1_800_000,
    "ODEI-APU":  1_900_000,
    "ODEI-HVC":  1_700_000,
    "ODEI-MOQ":  1_600_000,
    "ODEI-PAS":  1_500_000,
    "ODEI-TUM":  1_400_000,
    "ODEI-MAD":  1_500_000,
}

# ---------------------------------------------------------------------------
# Tasas de ejecución final 2025 (% sobre PIM)
# ---------------------------------------------------------------------------

UE_EJECUCION_PCT: dict[str, float] = {
    "OTIN":     97.2,  "DEC":      94.8,  "OTA":      91.5,  "OTPP":     88.3,
    "DNCPP":    95.1,  "DNCE":     86.7,  "DNEL":     82.4,  "DTI":      90.6,
    "ODEI-ARE": 96.4,  "ODEI-LAL": 93.2,  "ODEI-PIU": 89.8,  "ODEI-CUS": 94.7,
    "ODEI-JUN": 88.5,  "ODEI-LAM": 91.3,  "ODEI-CAJ": 84.6,  "ODEI-LIM": 87.9,
    "ODEI-ANC": 92.1,  "ODEI-PUN": 85.3,  "ODEI-ICA": 93.6,  "ODEI-SAM": 86.2,
    "ODEI-AYA": 79.4,  "ODEI-LOR": 83.7,  "ODEI-UCA": 88.1,  "ODEI-CAL": 90.5,
    "ODEI-HUC": 81.9,  "ODEI-TAC": 94.2,  "ODEI-AMA": 76.3,  "ODEI-APU": 78.8,
    "ODEI-HVC": 72.5,  "ODEI-MOQ": 95.8,  "ODEI-PAS": 74.1,  "ODEI-TUM": 91.7,
    "ODEI-MAD": 80.2,
}

# ---------------------------------------------------------------------------
# Clasificadores asignados por grupo de UEs.
# Cada entrada: (codigo_clasificador, porcentaje_del_pim_para_este_clasif)
# Los porcentajes de cada grupo deben sumar 1.0.
# ---------------------------------------------------------------------------

# OTIN — Oficina de Tecnologías de la Información
CLASIFS_OTIN: list[tuple[str, float]] = [
    ("2.3.2.2.2.3",   0.38),  # Equipos de computo
    ("2.3.2.7.2.99",  0.25),  # Software y licencias
    ("2.3.2.7.11.99", 0.15),  # Otros bienes TI
    ("2.3.2.4.1.1",   0.12),  # Mantenimiento equipos computo
    ("2.3.2.8.1.1",   0.07),  # Comunicaciones
    ("2.3.1.5.1.2",   0.03),  # Papeleria
]

# DEC, DNCE, DNEL, DNCPP — Estadística / Censos
CLASIFS_ESTADISTICA: list[tuple[str, float]] = [
    ("2.3.1.1.1.1",  0.20),  # Alimentos (para encuestadores)
    ("2.3.2.9.1.1",  0.25),  # Pasajes
    ("2.3.2.9.2.1",  0.20),  # Viaticos
    ("2.3.2.5.1.1",  0.15),  # Impresion y publicacion
    ("2.3.2.3.1.1",  0.20),  # Consultoria
]

# OTA, OTPP — Organización / Planificación
CLASIFS_ORGANIZACION: list[tuple[str, float]] = [
    ("2.3.2.3.1.1",  0.30),  # Consultoria
    ("2.3.1.5.1.2",  0.15),  # Papeleria
    ("2.3.2.5.1.1",  0.25),  # Impresion
    ("2.3.2.9.2.1",  0.30),  # Viaticos
]

# DTI — Dirección de Tecnología e Infraestructura
CLASIFS_DTI: list[tuple[str, float]] = [
    ("2.6.3.2.3.99",  0.40),  # Equipamiento y mobiliario
    ("2.3.2.4.1.99",  0.25),  # Mantenimiento infraestructura
    ("2.3.2.2.2.3",   0.20),  # Equipos de computo
    ("2.3.2.8.1.1",   0.10),  # Comunicaciones
    ("2.3.1.5.1.2",   0.05),  # Papeleria
]

# ODEIs — todas las oficinas regionales comparten la misma estructura
CLASIFS_ODEIS: list[tuple[str, float]] = [
    ("2.3.2.9.1.1",  0.28),  # Pasajes
    ("2.3.2.9.2.1",  0.27),  # Viaticos
    ("2.3.1.3.1.1",  0.20),  # Combustibles
    ("2.3.2.1.2.1",  0.15),  # Limpieza
    ("2.3.2.1.2.2",  0.10),  # Vigilancia
]

# Mapa: sigla → lista de clasificadores con porcentajes
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

# Las ODEIs se asignan dinámicamente (cualquier sigla que empiece con "ODEI-")

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------


def _dec(valor: float) -> Decimal:
    """Convierte un float a Decimal con 2 decimales (redondeo HALF_UP)."""
    return Decimal(str(valor)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _fuente_financiamiento(sigla: str, indice: int) -> str:
    """Determina la fuente de financiamiento.

    - Oficinas centrales: siempre 'Recursos Ordinarios'.
    - ODEIs: 80% 'Recursos Ordinarios', 20% 'Recursos Directamente Recaudados'
      (alternando por índice).
    """
    if sigla.startswith("ODEI-"):
        if indice % 5 == 4:  # cada 5.ª entrada → RDR
            return "Recursos Directamente Recaudados"
    return "Recursos Ordinarios"


def _calcular_montos(
    pim: float, pct_ejecucion: float, pct_clasif: float
) -> dict[str, Decimal]:
    """Calcula todos los montos presupuestales para un clasificador dado.

    Args:
        pim: PIM total de la UE en soles.
        pct_ejecucion: Porcentaje de ejecución final (0-100).
        pct_clasif: Porcentaje del PIM asignado a este clasificador (0-1).

    Returns:
        Diccionario con claves: pim, pia, devengado, certificado,
        compromiso_anual, girado, saldo.
    """
    pim_clasif = pim * pct_clasif
    devengado = pim_clasif * (pct_ejecucion / 100.0)

    return {
        "pia":              _dec(pim_clasif * 0.97),  # PIA ligeramente menor al PIM
        "pim":              _dec(pim_clasif),
        "devengado":        _dec(devengado),
        "certificado":      _dec(devengado * 1.02),   # certificado >= devengado
        "compromiso_anual": _dec(devengado * 1.01),   # compromiso >= devengado
        "girado":           _dec(devengado * 0.98),   # girado en trámite
        "saldo":            _dec(pim_clasif - devengado),
    }


def _calcular_meses(
    pim_clasif: Decimal, devengado_total: Decimal
) -> list[tuple[Decimal, Decimal, Decimal]]:
    """Genera los 12 registros mensuales (programado, ejecutado, saldo).

    La ejecución mensual se distribuye según MES_PESOS, ajustada para que
    la suma exacta coincida con devengado_total (sin errores de redondeo).

    Args:
        pim_clasif: PIM del clasificador (base para el programado uniforme).
        devengado_total: Devengado anual total que deben sumar los 12 meses.

    Returns:
        Lista de 12 tuplas (programado_mes, ejecutado_mes, saldo_mes).
    """
    programado_mes = _dec(float(pim_clasif) / 12.0)
    meses: list[tuple[Decimal, Decimal, Decimal]] = []
    ejecutado_acumulado = Decimal("0.00")

    for i, peso in enumerate(MES_PESOS):
        es_ultimo = i == 11

        if es_ultimo:
            # El mes 12 absorbe cualquier diferencia de centavos
            ejecutado_mes = devengado_total - ejecutado_acumulado
        else:
            ejecutado_mes = _dec(float(devengado_total) * peso)

        ejecutado_acumulado += ejecutado_mes
        saldo_mes = programado_mes - ejecutado_mes
        meses.append((programado_mes, ejecutado_mes, saldo_mes))

    return meses


# ---------------------------------------------------------------------------
# Función principal de sembrado
# ---------------------------------------------------------------------------


def sembrar_2025() -> None:
    """Siembra todos los datos de presupuesto 2025 en la base de datos.

    El proceso es idempotente: si detecta registros ProgramacionPresupuestal
    con anio=2025, termina inmediatamente sin modificar nada.
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
        if existentes > 0:
            print(
                f"[SKIP] Ya existen {existentes} registros ProgramacionPresupuestal "
                f"para el año {ANIO}. No se realizan cambios."
            )
            return

        # ------------------------------------------------------------------
        # PASO 1: Cargar UEs existentes y crear ODEIs faltantes
        # ------------------------------------------------------------------
        print(f"[1/5] Cargando unidades ejecutoras existentes...")

        # Índice por sigla para búsqueda rápida
        ues: dict[str, UnidadEjecutora] = {
            ue.sigla: ue for ue in session.query(UnidadEjecutora).all()
        }

        ues_creadas = 0
        for codigo, nombre, sigla, tipo in ODEIS_DATOS:
            if sigla not in ues:
                nueva_ue = UnidadEjecutora(
                    codigo=codigo,
                    nombre=nombre,
                    sigla=sigla,
                    tipo=tipo,
                    activo=True,
                )
                session.add(nueva_ue)
                ues[sigla] = nueva_ue
                ues_creadas += 1

        # Flush para que las nuevas UEs obtengan su id antes de usarlas como FK
        if ues_creadas > 0:
            session.flush()

        print(f"    UEs existentes: {len(ues) - ues_creadas} | ODEIs creadas: {ues_creadas}")

        # ------------------------------------------------------------------
        # PASO 2: Crear ClasificadorGasto faltantes
        # ------------------------------------------------------------------
        print(f"[2/5] Verificando clasificadores de gasto...")

        # Índice por codigo para búsqueda rápida
        clasifs: dict[str, ClasificadorGasto] = {
            c.codigo: c
            for c in session.query(ClasificadorGasto).all()
        }

        clasifs_creados = 0
        for codigo, descripcion, tipo_generico in CLASIFS_DATOS:
            if codigo not in clasifs:
                nuevo_clasif = ClasificadorGasto(
                    codigo=codigo,
                    descripcion=descripcion,
                    tipo_generico=tipo_generico,
                )
                session.add(nuevo_clasif)
                clasifs[codigo] = nuevo_clasif
                clasifs_creados += 1

        if clasifs_creados > 0:
            session.flush()

        print(f"    Clasificadores existentes: {len(clasifs) - clasifs_creados} | Creados: {clasifs_creados}")

        # ------------------------------------------------------------------
        # PASO 3: Crear metas presupuestales 2025 (1 por UE)
        # ------------------------------------------------------------------
        print(f"[3/5] Creando metas presupuestales {ANIO}...")

        # Cargar metas existentes del año (por ue_id + anio) para idempotencia parcial
        metas_existentes: dict[int, MetaPresupuestal] = {
            m.ue_id: m
            for m in session.query(MetaPresupuestal).filter(
                MetaPresupuestal.anio == ANIO
            ).all()
        }

        metas: dict[str, MetaPresupuestal] = {}
        metas_creadas = 0

        for sigla, ue in ues.items():
            if ue.id is None:
                # La UE fue recién creada pero aún no tiene id persistido
                session.flush()

            if ue.id in metas_existentes:
                metas[sigla] = metas_existentes[ue.id]
            else:
                codigo_meta = f"25{ue.id:04d}"  # e.g. "250001" — max 6 chars, within String(10)
                descripcion_meta = f"Meta Presupuestal {ANIO} - {ue.nombre}"

                nueva_meta = MetaPresupuestal(
                    codigo=codigo_meta,
                    descripcion=descripcion_meta,
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
        # PASO 4: Crear ProgramacionPresupuestal + ProgramacionMensual
        # ------------------------------------------------------------------
        print(f"[4/5] Generando programaciones presupuestales y mensuales {ANIO}...")

        total_pp = 0
        total_pm = 0

        # Procesamos en orden determinista (UEs con presupuesto definido primero)
        for indice_ue, sigla in enumerate(sorted(UE_BUDGET_2025.keys())):
            ue = ues.get(sigla)
            if ue is None:
                print(f"    [ADVERTENCIA] UE con sigla '{sigla}' no encontrada. Se omite.")
                continue

            meta = metas.get(sigla)
            if meta is None:
                print(f"    [ADVERTENCIA] Meta 2025 no encontrada para '{sigla}'. Se omite.")
                continue

            pim_ue = UE_BUDGET_2025[sigla]
            pct_ejecucion = UE_EJECUCION_PCT.get(sigla, 85.0)

            # Seleccionar lista de clasificadores según tipo de UE
            if sigla.startswith("ODEI-"):
                clasifs_ue = CLASIFS_ODEIS
            elif sigla in UE_CLASIFS_MAP:
                clasifs_ue = UE_CLASIFS_MAP[sigla]
            else:
                # Fallback genérico para UEs centrales no mapeadas
                clasifs_ue = CLASIFS_ESTADISTICA
                print(f"    [INFO] Usando clasificadores genéricos para '{sigla}'.")

            for indice_clasif, (codigo_clasif, pct_clasif) in enumerate(clasifs_ue):
                clasif = clasifs.get(codigo_clasif)
                if clasif is None:
                    print(
                        f"    [ADVERTENCIA] Clasificador '{codigo_clasif}' no encontrado. Se omite."
                    )
                    continue

                # Calcular montos anuales
                montos = _calcular_montos(pim_ue, pct_ejecucion, pct_clasif)
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
                session.flush()  # Obtener pp.id para las FK mensuales

                # Generar 12 registros mensuales
                registros_mensuales = _calcular_meses(montos["pim"], montos["devengado"])

                for mes_num, (programado, ejecutado, saldo) in enumerate(
                    registros_mensuales, start=1
                ):
                    pm = ProgramacionMensual(
                        programacion_presupuestal_id=pp.id,
                        mes=mes_num,
                        programado=programado,
                        ejecutado=ejecutado,
                        saldo=saldo,
                    )
                    session.add(pm)
                    total_pm += 1

                total_pp += 1

        # ------------------------------------------------------------------
        # PASO 5: Commit final
        # ------------------------------------------------------------------
        print(f"[5/5] Confirmando transacción...")
        session.commit()

        # ------------------------------------------------------------------
        # Resumen
        # ------------------------------------------------------------------
        print()
        print("=" * 60)
        print(f"  SEMBRADO {ANIO} COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        print(f"  UEs procesadas      : {len(ues)}")
        print(f"  ODEIs creadas       : {ues_creadas}")
        print(f"  Clasificadores creados: {clasifs_creados}")
        print(f"  Metas creadas       : {metas_creadas}")
        print(f"  ProgramacionPresupuestal: {total_pp}")
        print(f"  ProgramacionMensual     : {total_pm}")
        print("=" * 60)

    except Exception as exc:
        session.rollback()
        print(f"\n[ERROR] Se revirtio la transaccion. Detalle: {exc}")
        raise

    finally:
        session.close()


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sembrar_2025()
