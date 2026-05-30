"""
Script d'ingestion des données.
Lit le fichier CSV 'data/ventes.csv' avec Pandas,
puis écrit les données dans la table 'ventes_raw' de la base DuckDB 'ventes.duckdb'.
"""

import os
import pandas as pd
import duckdb


def ingest():
    """
    Charge le fichier CSV des ventes et l'insère dans une table DuckDB.

    - Source : data/ventes.csv
    - Destination : ventes.duckdb -> table 'ventes_raw'
    """
    # Déterminer le répertoire racine du projet (parent du dossier 'pipeline/')
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    csv_path = os.path.join(base_dir, "data", "ventes.csv")
    db_path = os.path.join(base_dir, "ventes.duckdb")

    # ---- Lecture du CSV avec Pandas ----
    print(f"[INGEST] Lecture du fichier CSV : {csv_path}")
    df = pd.read_csv(csv_path)

    # Nettoyage des noms de colonnes (suppression des espaces superflus)
    df.columns = df.columns.str.strip()
    print(f"[INGEST] {len(df)} lignes chargées. Colonnes : {list(df.columns)}")

    # ---- Écriture dans DuckDB ----
    print(f"[INGEST] Écriture dans la base DuckDB : {db_path}")
    con = duckdb.connect(db_path)
    con.execute("DROP TABLE IF EXISTS ventes_raw")
    con.execute("CREATE TABLE ventes_raw AS SELECT * FROM df")
    print("[INGEST] Table 'ventes_raw' créée avec succès.")

    # Vérification rapide
    result = con.execute("SELECT COUNT(*) FROM ventes_raw").fetchone()
    print(f"[INGEST] Vérification : {result[0]} lignes dans 'ventes_raw'.")
    con.close()


if __name__ == "__main__":
    ingest()
