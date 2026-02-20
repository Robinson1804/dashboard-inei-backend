"""Seed demo data for Contratos Menores, Adquisiciones, Proveedores, and Alertas.

Works with whatever UEs and Metas already exist in the database (e.g., from
Excel imports). Run this AFTER uploading the example Excel files.

Usage:
    py seed_demo_transactions.py
"""

from __future__ import annotations

import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal  # noqa: E402
from app.models import (  # noqa: E402
    Adquisicion,
    AdquisicionDetalle,
    AdquisicionProceso,
    Alerta,
    ContratoMenor,
    ContratoMenorProceso,
    Proveedor,
    MetaPresupuestal,
    UnidadEjecutora,
)

ANIO = 2026


def _d(y, m, d):
    return date(y, m, d)


def _dec(v):
    return Decimal(str(round(v, 2)))


def seed_proveedores(session) -> list[Proveedor]:
    if session.query(Proveedor).count() > 0:
        print("  [SKIP] Proveedor — ya tiene datos.")
        return session.query(Proveedor).all()

    registros = [
        Proveedor(ruc="20100070970", razon_social="IBM DEL PERU S.A.C.",
                  nombre_comercial="IBM Peru", estado_rnp="HABIDO", activo=True),
        Proveedor(ruc="20112273922", razon_social="MICROSOFT PERU S.R.L.",
                  nombre_comercial="Microsoft Peru", estado_rnp="HABIDO", activo=True),
        Proveedor(ruc="20501503893", razon_social="SUMINISTROS PAPELEROS LA UNION S.A.C.",
                  nombre_comercial="Papeleria La Union", estado_rnp="HABIDO", activo=True),
        Proveedor(ruc="20602913771", razon_social="SOLUCIONES TECNOLOGICAS ANDINAS S.A.C.",
                  nombre_comercial="STA Soluciones TI", estado_rnp="HABIDO", activo=True),
        Proveedor(ruc="20536987412", razon_social="CONSULTORA ESTADISTICA PERU S.A.C.",
                  nombre_comercial="CEP Consultores", estado_rnp="HABIDO", activo=True),
        Proveedor(ruc="20600347851", razon_social="ELECTRO SERVICIOS GENERALES J&M E.I.R.L.",
                  nombre_comercial="Electro Servicios J&M", estado_rnp="HABIDO", activo=True),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Proveedor — {len(registros)} registros.")
    return session.query(Proveedor).all()


def seed_contratos_menores(session, ue_id, meta_id, proveedores):
    if session.query(ContratoMenor).count() > 0:
        print("  [SKIP] ContratoMenor — ya tiene datos.")
        return session.query(ContratoMenor).all()

    prov_map = {p.ruc: p.id for p in proveedores}

    datos = [
        ("CM-2026-001", "Papel Bond A4 para Impresoras — 200 Millar",
         "BIEN", "MATERIALES_OFICINA", "PAGADO", 4200, 4080, "20501503893", "OC-2026-001", 3),
        ("CM-2026-002", "Servicio Limpieza Oficinas — Enero 2026",
         "SERVICIO", "LIMPIEZA", "PAGADO", 3800, 3800, "20600347851", "OS-2026-001", 3),
        ("CM-2026-003", "Cartuchos Tinta y Toner para Impresoras",
         "BIEN", "SUMINISTROS_TI", "EJECUTADO", 5500, 5350, "20602913771", "OC-2026-003", 3),
        ("CM-2026-004", "Servicio Fotocopiado Materiales Censales",
         "SERVICIO", "MATERIALES_CENSALES", "ORDEN_EMITIDA", 8900, None, "20501503893", "OS-2026-004", 3),
        ("CM-2026-005", "Combustible para Vehiculos Operativo",
         "BIEN", "COMBUSTIBLES", "EN_PROCESO", 12000, None, None, None, 2),
        ("CM-2026-006", "Servicio Mensajeria y Courier Nacional",
         "SERVICIO", "MENSAJERIA", "PAGADO", 2800, 2750, "20600347851", "OS-2026-006", 3),
        ("CM-2026-007", "Materiales de Limpieza Areas de Trabajo",
         "BIEN", "MATERIALES_LIMPIEZA", "PAGADO", 1850, 1820, "20600347851", "OC-2026-007", 3),
        ("CM-2026-008", "Diseno e Impresion Brochures Difusion",
         "SERVICIO", "MATERIALES_DIFUSION", "EN_PROCESO", 7200, None, "20501503893", "OS-2026-008", 2),
        ("CM-2026-009", "Utiles Escritorio y Articulos de Oficina",
         "BIEN", "MATERIALES_OFICINA", "PENDIENTE", 3100, None, None, None, 0),
        ("CM-2026-010", "Mantenimiento Reparacion Mobiliario Oficinas",
         "SERVICIO", "MANTENIMIENTO", "ORDEN_EMITIDA", 6500, None, "20600347851", "OS-2026-010", 3),
    ]

    registros = []
    for cod, desc, tipo, cat, estado, m_est, m_ej, ruc, n_ord, n_cot in datos:
        registros.append(ContratoMenor(
            codigo=cod, anio=ANIO, ue_id=ue_id, meta_id=meta_id,
            descripcion=desc, tipo_objeto=tipo, categoria=cat, estado=estado,
            monto_estimado=_dec(m_est), monto_ejecutado=_dec(m_ej) if m_ej else None,
            proveedor_id=prov_map.get(ruc), n_orden=n_ord, n_cotizaciones=n_cot,
        ))

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ContratoMenor — {len(registros)} registros.")
    return session.query(ContratoMenor).order_by(ContratoMenor.codigo).all()


def seed_contrato_procesos(session, contratos):
    if session.query(ContratoMenorProceso).count() > 0:
        print("  [SKIP] ContratoMenorProceso — ya tiene datos.")
        return

    plantilla = [
        (1, "Elaboracion del Requerimiento", "AREA_USUARIA", 2),
        (2, "Solicitud de Cotizaciones", "LOGISTICA", 3),
        (3, "Recepcion y Evaluacion de Cotizaciones", "LOGISTICA", 2),
        (4, "Seleccion del Proveedor", "LOGISTICA", 1),
        (5, "Aprobacion del Cuadro Comparativo", "PRESUPUESTO", 1),
        (6, "Emision de la Orden", "LOGISTICA", 1),
        (7, "Ejecucion y Entrega", "PROVEEDOR", 7),
        (8, "Conformidad del Area Usuaria", "AREA_USUARIA", 2),
        (9, "Tramitacion del Pago", "LOGISTICA", 3),
    ]

    progreso = {
        "PENDIENTE": 0, "EN_PROCESO": 3, "ORDEN_EMITIDA": 6,
        "EJECUTADO": 8, "PAGADO": 9,
    }

    registros = []
    for contrato in contratos:
        pasos_ok = progreso.get(contrato.estado, 0)
        fecha = _d(ANIO, 1, 20)

        for orden, hito, area, dias in plantilla:
            f_ini = fecha
            f_fin = fecha + timedelta(days=dias)

            if orden <= pasos_ok:
                estado = "COMPLETADO"
            elif orden == pasos_ok + 1 and contrato.estado != "PENDIENTE":
                estado = "EN_CURSO"
            else:
                estado = "PENDIENTE"

            registros.append(ContratoMenorProceso(
                contrato_menor_id=contrato.id, orden=orden, hito=hito,
                area_responsable=area, dias_planificados=dias,
                fecha_inicio=f_ini, fecha_fin=f_fin, estado=estado,
            ))
            fecha = f_fin + timedelta(days=1)

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ContratoMenorProceso — {len(registros)} registros.")


def seed_adquisiciones(session, ue_id, meta_id, proveedores):
    if session.query(Adquisicion).count() > 0:
        print("  [SKIP] Adquisicion — ya tiene datos.")
        return session.query(Adquisicion).all()

    prov_map = {p.ruc: p.id for p in proveedores}

    datos = [
        ("ADQ-2026-001", "Servidores Alto Rendimiento Centro de Datos",
         "BIEN", "LICITACION_PUBLICA", "EN_EJECUCION", "EJECUCION_CONTRACTUAL",
         350000, 338500, "20602913771"),
        ("ADQ-2026-002", "Impresion y Distribucion Formularios Censales",
         "SERVICIO", "CONCURSO_PUBLICO", "ADJUDICADO", "EJECUCION_CONTRACTUAL",
         280000, 271500, "20100070970"),
        ("ADQ-2026-003", "Licencias Corporativas Microsoft 365",
         "BIEN", "CATALOGO_ELECTRONICO", "CULMINADO", "EJECUCION_CONTRACTUAL",
         145000, 138700, "20112273922"),
        ("ADQ-2026-004", "Consultoria Sistema de Cuentas Nacionales",
         "CONSULTORIA", "CONCURSO_PUBLICO", "EN_SELECCION", "SELECCION",
         320000, None, None),
        ("ADQ-2026-005", "Equipos Comunicaciones y Redes Oficinas Regionales",
         "BIEN", "LICITACION_PUBLICA", "EN_ACTOS_PREPARATORIOS", "ACTUACIONES_PREPARATORIAS",
         210000, None, None),
        ("ADQ-2026-006", "Servicio Encuestadores ENAHO 2026",
         "SERVICIO", "CONCURSO_PUBLICO", "EN_EJECUCION", "EJECUCION_CONTRACTUAL",
         480000, 465000, "20536987412"),
        ("ADQ-2026-007", "Tablets para Empadronadores Censales",
         "BIEN", "LICITACION_PUBLICA", "DESIERTO", "SELECCION",
         520000, None, None),
        ("ADQ-2026-008", "Mantenimiento Vehiculos Oficiales",
         "SERVICIO", "COMPARACION_PRECIOS", "EN_SELECCION", "SELECCION",
         58000, None, None),
    ]

    registros = []
    for cod, desc, tipo, proc, estado, fase, m_ref, m_adj, ruc in datos:
        registros.append(Adquisicion(
            codigo=cod, anio=ANIO, ue_id=ue_id, meta_id=meta_id,
            descripcion=desc, tipo_objeto=tipo, tipo_procedimiento=proc,
            estado=estado, fase_actual=fase,
            monto_referencial=_dec(m_ref),
            monto_adjudicado=_dec(m_adj) if m_adj else None,
            proveedor_id=prov_map.get(ruc),
        ))

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Adquisicion — {len(registros)} registros.")
    return session.query(Adquisicion).order_by(Adquisicion.codigo).all()


def seed_adquisicion_detalles(session, adquisiciones):
    if session.query(AdquisicionDetalle).count() > 0:
        print("  [SKIP] AdquisicionDetalle — ya tiene datos.")
        return

    registros = []
    for idx, adq in enumerate(adquisiciones, start=1):
        has_seace = adq.estado not in ("EN_ACTOS_PREPARATORIOS",)
        registros.append(AdquisicionDetalle(
            adquisicion_id=adq.id,
            n_expediente=f"EXP-{ANIO}-{idx:03d}",
            n_proceso_seace=f"N-{ANIO}-1-{idx:04d}" if has_seace else None,
        ))

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] AdquisicionDetalle — {len(registros)} registros.")


def seed_adquisicion_procesos(session, adquisiciones):
    if session.query(AdquisicionProceso).count() > 0:
        print("  [SKIP] AdquisicionProceso — ya tiene datos.")
        return

    plantilla = [
        (1, "Elaboracion Requerimiento Tecnico", "ACTUACIONES_PREPARATORIAS", "AREA_USUARIA", 5),
        (2, "Conformidad Area Usuaria", "ACTUACIONES_PREPARATORIAS", "AREA_USUARIA", 3),
        (3, "Estudio de Mercado", "ACTUACIONES_PREPARATORIAS", "LOGISTICA", 7),
        (4, "Valor Referencial", "ACTUACIONES_PREPARATORIAS", "LOGISTICA", 3),
        (5, "Expediente de Contratacion", "ACTUACIONES_PREPARATORIAS", "LOGISTICA", 5),
        (6, "Aprobacion Expediente", "ACTUACIONES_PREPARATORIAS", "PRESUPUESTO", 3),
        (7, "Designacion Comite Seleccion", "ACTUACIONES_PREPARATORIAS", "PRESUPUESTO", 2),
        (8, "Elaboracion y Aprobacion Bases", "ACTUACIONES_PREPARATORIAS", "COMITE", 7),
        (9, "Convocatoria SEACE", "SELECCION", "COMITE", 1),
        (10, "Registro Participantes", "SELECCION", "COMITE", 5),
        (11, "Consultas y Observaciones", "SELECCION", "COMITE", 5),
        (12, "Absolucion Consultas", "SELECCION", "COMITE", 7),
        (13, "Integracion de Bases", "SELECCION", "COMITE", 3),
        (14, "Presentacion Ofertas", "SELECCION", "PROVEEDOR", 5),
        (15, "Evaluacion Ofertas", "SELECCION", "COMITE", 5),
        (16, "Otorgamiento Buena Pro", "SELECCION", "COMITE", 1),
        (17, "Consentimiento Buena Pro", "EJECUCION_CONTRACTUAL", "COMITE", 5),
        (18, "Suscripcion Contrato", "EJECUCION_CONTRACTUAL", "LOGISTICA", 5),
        (19, "Entrega Adelanto", "EJECUCION_CONTRACTUAL", "LOGISTICA", 3),
        (20, "Ejecucion Prestacion", "EJECUCION_CONTRACTUAL", "PROVEEDOR", 30),
        (21, "Conformidad Prestacion", "EJECUCION_CONTRACTUAL", "AREA_USUARIA", 5),
        (22, "Pago al Proveedor", "EJECUCION_CONTRACTUAL", "LOGISTICA", 5),
    ]

    progreso = {
        "EN_ACTOS_PREPARATORIOS": 4,
        "EN_SELECCION": 10,
        "EN_EJECUCION": 19,
        "ADJUDICADO": 17,
        "CULMINADO": 22,
        "DESIERTO": 14,
    }

    registros = []
    for adq in adquisiciones:
        pasos_ok = progreso.get(adq.estado, 0)
        fecha = _d(ANIO, 1, 15)

        for orden, hito, fase, area, dias in plantilla:
            f_ini = fecha
            f_fin = fecha + timedelta(days=dias)

            if orden <= pasos_ok:
                estado = "COMPLETADO"
                f_real_ini, f_real_fin = f_ini, f_fin
            elif orden == pasos_ok + 1 and adq.estado not in ("DESIERTO", "CULMINADO"):
                estado = "EN_CURSO"
                f_real_ini, f_real_fin = f_ini, None
            else:
                estado = "PENDIENTE"
                f_real_ini, f_real_fin = None, None

            if adq.estado == "DESIERTO" and orden == 15:
                estado = "OBSERVADO"

            registros.append(AdquisicionProceso(
                adquisicion_id=adq.id, orden=orden, hito=hito, fase=fase,
                area_responsable=area, dias_planificados=dias,
                fecha_inicio=f_ini, fecha_fin=f_fin,
                fecha_real_inicio=f_real_ini, fecha_real_fin=f_real_fin,
                estado=estado,
            ))
            fecha = f_fin + timedelta(days=1)

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] AdquisicionProceso — {len(registros)} registros.")


def seed_alertas(session, ue_id):
    if session.query(Alerta).count() > 0:
        print("  [SKIP] Alerta — ya tiene datos.")
        return

    ahora = datetime(ANIO, 2, 18, 9, 0, 0)
    registros = [
        Alerta(tipo="EJECUCION_BAJA", nivel="ROJO",
               titulo="Ejecucion Presupuestal Critica",
               descripcion="Ejecucion del 55.8% en combustibles, por debajo del umbral minimo del 70%.",
               ue_id=ue_id, modulo="PRESUPUESTO", entidad_tipo="programacion",
               leida=False, resuelta=False, fecha_generacion=ahora - timedelta(hours=2)),
        Alerta(tipo="PROCESO_PARALIZADO", nivel="ROJO",
               titulo="ADQ-2026-005 Sin Avance por 15 Dias",
               descripcion="Proceso de equipos de comunicaciones sin avance en actuaciones preparatorias.",
               ue_id=ue_id, modulo="ADQUISICIONES", entidad_tipo="adquisicion",
               leida=False, resuelta=False, fecha_generacion=ahora - timedelta(hours=1)),
        Alerta(tipo="PROCESO_DESIERTO", nivel="AMARILLO",
               titulo="ADQ-2026-007 Declarado Desierto",
               descripcion="Licitacion de tablets para empadronadores declarada desierta.",
               ue_id=ue_id, modulo="ADQUISICIONES", entidad_tipo="adquisicion",
               leida=False, resuelta=False, fecha_generacion=ahora - timedelta(days=2)),
        Alerta(tipo="FRACCIONAMIENTO_DETECTADO", nivel="ROJO",
               titulo="Posible Fraccionamiento — Materiales Oficina",
               descripcion="3 contratos menores en MATERIALES_OFICINA superan 8 UIT acumulado.",
               ue_id=ue_id, modulo="CONTRATOS_MENORES", entidad_tipo="contrato_menor",
               leida=False, resuelta=False, fecha_generacion=ahora - timedelta(hours=3)),
        Alerta(tipo="META_CUMPLIDA", nivel="VERDE",
               titulo="Meta Cumplida — Licencias Microsoft",
               descripcion="ADQ-2026-003 culminado exitosamente con 95.6% del monto referencial.",
               ue_id=ue_id, modulo="ADQUISICIONES", entidad_tipo="adquisicion",
               leida=True, resuelta=True, fecha_generacion=ahora - timedelta(days=5)),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Alerta — {len(registros)} registros.")


def main():
    print("=" * 60)
    print("  Seed Demo Transactions (post-Excel import)")
    print("=" * 60)

    session = SessionLocal()
    try:
        # Get first UE and Meta from DB (created by Excel import)
        ue = session.query(UnidadEjecutora).first()
        meta = session.query(MetaPresupuestal).first()

        if not ue:
            print("\n[ERROR] No hay UEs en la BD. Primero importe CUADRO_AO_META.")
            return
        if not meta:
            print("\n[ERROR] No hay Metas en la BD. Primero importe CUADRO_AO_META.")
            return

        print(f"\n  Usando UE: {ue.codigo} - {ue.nombre} (id={ue.id})")
        print(f"  Usando Meta: {meta.codigo} - {meta.descripcion} (id={meta.id})")

        print("\n[1/6] Proveedores...")
        proveedores = seed_proveedores(session)

        print("\n[2/6] Contratos Menores...")
        contratos = seed_contratos_menores(session, ue.id, meta.id, proveedores)

        print("\n[3/6] Contrato Menor Procesos...")
        seed_contrato_procesos(session, contratos)

        print("\n[4/6] Adquisiciones...")
        adquisiciones = seed_adquisiciones(session, ue.id, meta.id, proveedores)

        print("\n[5/6] Adquisicion Detalles + Procesos...")
        seed_adquisicion_detalles(session, adquisiciones)
        seed_adquisicion_procesos(session, adquisiciones)

        print("\n[6/6] Alertas...")
        seed_alertas(session, ue.id)

        session.commit()
        print("\n" + "=" * 60)
        print("  Seed completado! Contratos, adquisiciones y alertas creados.")
        print("=" * 60)

    except Exception as exc:
        session.rollback()
        print(f"\n[ERROR] Seed fallido: {exc}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
