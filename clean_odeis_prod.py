"""Limpieza de UEs ODEI de la base de datos de produccion.

Uso:
    py clean_odeis_prod.py <DATABASE_URL>

Ejemplo:
    py clean_odeis_prod.py "postgresql://postgres:PASS@host:PORT/railway"

Este script NO usa pydantic-settings ni lee archivos .env.
La URL de BD se pasa como argumento de linea de comandos.
"""

from __future__ import annotations

import sys

if len(sys.argv) < 2:
    print("ERROR: Se requiere DATABASE_URL como argumento.")
    print("Uso: py clean_odeis_prod.py <DATABASE_URL>")
    sys.exit(1)

DATABASE_URL = sys.argv[1]
print(f"[INFO] Conectando a: {DATABASE_URL[:40]}...")

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

def clean_odeis():
    session = Session()
    try:
        # Identificar IDs de UEs ODEI
        result = session.execute(
            text("SELECT id, sigla FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%' ORDER BY sigla")
        )
        odei_ues = result.fetchall()

        if not odei_ues:
            print("[INFO] No se encontraron UEs ODEI en la base de datos.")
            return

        odei_ids = [row[0] for row in odei_ues]
        print(f"[INFO] Encontradas {len(odei_ues)} UEs ODEI:")
        for row in odei_ues:
            print(f"       ID={row[0]}  sigla={row[1]}")

        confirm = input(f"\n¿Eliminar estas {len(odei_ues)} UEs ODEI y todos sus datos? [s/N]: ").strip().lower()
        if confirm != 's':
            print("[ABORT] Operacion cancelada.")
            return

        print("\n[STEP 1] Eliminando adquisicion_proceso de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM adquisicion_proceso
            WHERE adquisicion_id IN (
                SELECT id FROM adquisicion WHERE ue_id = ANY(:ids)
            )
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 2] Eliminando adquisicion_detalle de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM adquisicion_detalle
            WHERE adquisicion_id IN (
                SELECT id FROM adquisicion WHERE ue_id = ANY(:ids)
            )
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 3] Eliminando adquisicion de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM adquisicion WHERE ue_id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 4] Eliminando contrato_menor_proceso de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM contrato_menor_proceso
            WHERE contrato_menor_id IN (
                SELECT id FROM contrato_menor WHERE ue_id = ANY(:ids)
            )
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 5] Eliminando contrato_menor de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM contrato_menor WHERE ue_id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 6] Eliminando actividad_operativa de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM actividad_operativa WHERE ue_id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 7] Eliminando alertas de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM alerta WHERE ue_id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 8] Eliminando programacion_mensual de ODEI metas...")
        r = session.execute(text("""
            DELETE FROM programacion_mensual
            WHERE programacion_presupuestal_id IN (
                SELECT pp.id FROM programacion_presupuestal pp
                JOIN meta_presupuestal mp ON pp.meta_presupuestal_id = mp.id
                WHERE mp.ue_id = ANY(:ids)
            )
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 9] Eliminando programacion_presupuestal de ODEI metas...")
        r = session.execute(text("""
            DELETE FROM programacion_presupuestal
            WHERE meta_presupuestal_id IN (
                SELECT id FROM meta_presupuestal WHERE ue_id = ANY(:ids)
            )
        """), {"ids": odei_ids})
        print(f"         Eliminados: {r.rowcount} registros")

        print("[STEP 10] Eliminando meta_presupuestal de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM meta_presupuestal WHERE ue_id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"          Eliminados: {r.rowcount} registros")

        print("[STEP 11] Eliminando usuario de ODEI UEs...")
        r = session.execute(text("""
            DELETE FROM usuario WHERE ue_id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"          Eliminados: {r.rowcount} registros")

        print("[STEP 12] Eliminando unidad_ejecutora ODEI...")
        r = session.execute(text("""
            DELETE FROM unidad_ejecutora WHERE id = ANY(:ids)
        """), {"ids": odei_ids})
        print(f"          Eliminados: {r.rowcount} UEs ODEI")

        session.commit()
        print("\n[OK] Limpieza completada exitosamente.")

        # Verificar estado final
        result = session.execute(text("SELECT COUNT(*) FROM unidad_ejecutora"))
        total = result.scalar()
        print(f"[OK] Total UEs restantes en BD: {total}")

        result = session.execute(text("SELECT sigla FROM unidad_ejecutora ORDER BY sigla"))
        ues = [row[0] for row in result.fetchall()]
        print(f"[OK] UEs: {', '.join(ues)}")

    except Exception as e:
        session.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    clean_odeis()
