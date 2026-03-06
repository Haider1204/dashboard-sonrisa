"""
procesar_datos.py
=================
Fundación Operación Sonrisa — Pipeline de datos
------------------------------------------------
Uso:
    python procesar_datos.py                        # procesa datos/datos.xlsx
    python procesar_datos.py datos/mi_archivo.xlsx  # ruta personalizada

Genera:  docs/data.json  (leído por el dashboard)
"""

import sys
import json
import openpyxl
from pathlib import Path
from collections import defaultdict
from datetime import date

# ── Configuración ────────────────────────────────────────────────────────────
EXCEL_DEFAULT = Path("datos/datos.xlsx")
OUTPUT_PATH   = Path("docs/data.json")

MES_ORDER = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]

TRIM_LABELS = {
    "Q1": "Q1 (Jul–Sep)",
    "Q2": "Q2 (Oct–Dic)",
    "Q3": "Q3 (Ene–Feb)",
    "Q4": "Q4 (Mar–Jun)",
}

# ── Lectura del Excel ─────────────────────────────────────────────────────────
def leer_excel(path: Path) -> list[dict]:
    print(f"📂 Leyendo: {path}")
    wb = openpyxl.load_workbook(path)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    headers = rows[0]
    data = [dict(zip(headers, r)) for r in rows[1:] if any(r)]
    print(f"   → {len(data)} filas cargadas")
    return data

# ── Deduplicación ─────────────────────────────────────────────────────────────
# El Excel repite demografía/pacientes por cada especialidad del mismo mes/depto.
# Tomamos UN registro único por (Departamento, Ciudad, Mes, Año).
def deduplicar(data: list[dict]) -> dict:
    unicos = {}
    for r in data:
        key = (r["Programa/Departamento"], r["Programa/Ciudad"], r["Mes"], r["Año"])
        if key not in unicos:
            unicos[key] = {
                "depto":      r["Programa/Departamento"],
                "ciudad":     r["Programa/Ciudad"],
                "mes":        r["Mes"],
                "año":        r["Año"],
                "trimestre":  r["Trimestre Fiscal"],
                "ninos":      int(r["Niños"]               or 0),
                "ninas":      int(r["Niñas"]               or 0),
                "adultos_f":  int(r["Adultos Femeninos"]   or 0),
                "adultos_m":  int(r["Adultos Masculinos"]  or 0),
                "migrantes":  int(r["Pacientes Migrantes"] or 0),
                "indigenas":  int(r["Pacientes Indigenas"] or 0),
                "nuevos":     int(r["Pacientes Nuevos"]    or 0),
                "unicos":     int(r["Pacientes únicos"]    or 0),
                "operados":   int(r["Pacientes Operados"]  or 0),
                "procedimientos": int(r["Procedimientos Realizados"] or 0),
            }
    return unicos

# ── Agregaciones ──────────────────────────────────────────────────────────────
def calcular_kpis(unicos: dict) -> dict:
    vals = list(unicos.values())
    return {
        "total_pacientes":     sum(v["unicos"]       for v in vals),
        "total_operados":      sum(v["operados"]     for v in vals),
        "total_procedimientos":sum(v["procedimientos"]for v in vals),
        "total_nuevos":        sum(v["nuevos"]       for v in vals),
        "migrantes":           sum(v["migrantes"]    for v in vals),
        "indigenas":           sum(v["indigenas"]    for v in vals),
    }

def calcular_demografia(unicos: dict) -> dict:
    vals = list(unicos.values())
    return {
        "ninos":    sum(v["ninos"]    for v in vals),
        "ninas":    sum(v["ninas"]    for v in vals),
        "adultos_f":sum(v["adultos_f"]for v in vals),
        "adultos_m":sum(v["adultos_m"]for v in vals),
    }

def calcular_tendencia(unicos: dict, meses_presentes: list) -> list:
    mes_agg = defaultdict(lambda: dict(unicos=0,operados=0,procedimientos=0,migrantes=0,indigenas=0,nuevos=0))
    for v in unicos.values():
        m = v["mes"]
        for campo in ["unicos","operados","procedimientos","migrantes","indigenas","nuevos"]:
            mes_agg[m][campo] += v[campo]
    return [{"mes": m, **mes_agg[m]} for m in meses_presentes if m in mes_agg]

def calcular_especialidades(data: list[dict]) -> list:
    esp = defaultdict(int)
    for r in data:
        esp[r["Especialización"]] += int(r["Valor"] or 0)
    return [{"nombre": k, "citas": v}
            for k, v in sorted(esp.items(), key=lambda x: -x[1]) if v > 0]

def calcular_departamentos(unicos: dict) -> list:
    depto_agg = defaultdict(lambda: dict(ciudad="", unicos=0, operados=0, procedimientos=0))
    for v in unicos.values():
        d = v["depto"]
        depto_agg[d]["ciudad"] = v["ciudad"]
        for campo in ["unicos","operados","procedimientos"]:
            depto_agg[d][campo] += v[campo]
    return [{"depto": k, **v}
            for k, v in sorted(depto_agg.items(), key=lambda x: -x[1]["unicos"])]

def calcular_trimestres(unicos: dict) -> list:
    trim_agg = defaultdict(lambda: dict(label="", unicos=0, operados=0, procedimientos=0, nuevos=0))
    for v in unicos.values():
        t = v["trimestre"]
        trim_agg[t]["label"] = TRIM_LABELS.get(t, t)
        for campo in ["unicos","operados","procedimientos","nuevos"]:
            trim_agg[t][campo] += v[campo]
    order = ["Q1","Q2","Q3","Q4"]
    return [{"trimestre": t, **trim_agg[t]}
            for t in order if t in trim_agg]

def calcular_citas_totales(data: list[dict]) -> int:
    return sum(int(r["Valor"] or 0) for r in data)

# ── Pipeline principal ────────────────────────────────────────────────────────
def main():
    excel_path = Path(sys.argv[1]) if len(sys.argv) > 1 else EXCEL_DEFAULT

    if not excel_path.exists():
        print(f"❌ No se encontró el archivo: {excel_path}")
        print("   Coloca tu Excel en la carpeta 'datos/' con el nombre 'datos.xlsx'")
        sys.exit(1)

    data   = leer_excel(excel_path)
    unicos = deduplicar(data)

    # Detectar meses presentes en orden cronológico
    meses_presentes = [m for m in MES_ORDER if any(v["mes"] == m for v in unicos.values())]

    kpis = calcular_kpis(unicos)
    kpis["total_citas"] = calcular_citas_totales(data)

    output = {
        "generado":      str(date.today()),
        "kpis":          kpis,
        "demografia":    calcular_demografia(unicos),
        "tendencia":     calcular_tendencia(unicos, meses_presentes),
        "especialidades":calcular_especialidades(data),
        "departamentos": calcular_departamentos(unicos),
        "trimestres":    calcular_trimestres(unicos),
        "meses_orden":   meses_presentes,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✅ data.json generado en: {OUTPUT_PATH}")
    print(f"   Pacientes únicos  : {kpis['total_pacientes']:,}")
    print(f"   Operados          : {kpis['total_operados']:,}")
    print(f"   Procedimientos    : {kpis['total_procedimientos']:,}")
    print(f"   Citas totales     : {kpis['total_citas']:,}")
    print(f"   Meses procesados  : {', '.join(meses_presentes)}")
    print("\n🚀 Ahora sube los cambios a GitHub:")
    print("   git add docs/data.json")
    print("   git commit -m 'datos: actualización mensual'")
    print("   git push")

if __name__ == "__main__":
    main()
