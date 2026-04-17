from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ArbresRemarquables").getOrCreate()

df = spark.read.parquet("arbresremarquablesparis.parquet")
rdd = df.rdd

annee_actuelle = 2026

# ── 1. Âge du plus vieil arbre ──────────────────────────────────────────────
def annee_valide(row):
    val = row["com_annee_plantation"]
    if val is None:
        return False
    try:
        int(val)
        return True
    except (ValueError, TypeError):
        return False

age_max = (
    rdd
    .filter(annee_valide)
    .map(lambda row: annee_actuelle - int(row["com_annee_plantation"]))
    .reduce(lambda a, b: a if a > b else b)
)
print(f"Âge du plus vieil arbre : {age_max} ans")


# ── 2. Hauteur moyenne de tous les arbres ───────────────────────────────────
def hauteur_valide(row):
    val = row["arbres_hauteurenm"]
    if val is None:
        return False
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False

rdd_hauteur = (
    rdd
    .filter(hauteur_valide)
    .map(lambda row: (float(row["arbres_hauteurenm"]), 1))
)

total_hauteur, total_count = rdd_hauteur.reduce(lambda a, b: (a[0]+b[0], a[1]+b[1]))
moyenne = total_hauteur / total_count
print(f"Hauteur moyenne : {moyenne:.2f} m")


# ── 3. Arrondissement avec le plus d'arbres ─────────────────────────────────
def arrond_valide(row):
    val = row["arbres_arrondissement"]
    if val is None:
        return False
    try:
        str(val).strip()
        return True
    except (ValueError, TypeError):
        return False

arrond_max = (
    rdd
    .filter(arrond_valide)
    .map(lambda row: (row["arbres_arrondissement"], 1))
    .reduceByKey(lambda a, b: a + b)
    .reduce(lambda a, b: a if a[1] > b[1] else b)
)
print(f"Arrondissement avec le plus d'arbres : {arrond_max[0]} ({arrond_max[1]} arbres)")