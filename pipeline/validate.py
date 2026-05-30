"""
Script de validation des données.
Vérifie la présence des colonnes requises dans la table 'ventes_raw'
et l'absence de valeurs NULL sur les champs clés.
"""

import os
import duckdb


# Colonnes attendues dans la table 'ventes_raw'
REQUIRED_COLUMNS = {"date", "produit", "categorie", "quantite", "prix_unitaire", "ville"}

# Colonnes sur lesquelles les valeurs NULL ne sont pas tolérées
NOT_NULL_COLUMNS = ["date", "produit", "categorie", "quantite", "prix_unitaire"]


def validate():
    """
    Valide les données de la table 'ventes_raw' dans DuckDB.

    Étapes :
    1. Vérifie que toutes les colonnes requises sont présentes.
    2. Vérifie l'absence de valeurs NULL sur les champs essentiels.

    Lève ValueError si une des vérifications échoue.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, "ventes.duckdb")

    print(f"[VALIDATE] Connexion à la base DuckDB : {db_path}")
    con = duckdb.connect(db_path)

    # ---- Étape 1 : Vérification des colonnes ----
    print("[VALIDATE] Vérification des colonnes requises...")
    columns_info = con.execute("DESCRIBE ventes_raw").fetchall()
    actual_columns = {col[0] for col in columns_info}

    missing_columns = REQUIRED_COLUMNS - actual_columns
    if missing_columns:
        con.close()
        raise ValueError(
            f"[VALIDATE] ERREUR : Colonnes manquantes dans 'ventes_raw' : {missing_columns}"
        )
    print("[VALIDATE] ✅ Toutes les colonnes requises sont présentes.")

    # ---- Étape 2 : Vérification des valeurs NULL ----
    print("[VALIDATE] Vérification de l'absence de valeurs NULL sur les champs clés...")
    for col in NOT_NULL_COLUMNS:
        query = f"SELECT COUNT(*) FROM ventes_raw WHERE {col} IS NULL"
        null_count = con.execute(query).fetchone()[0]
        if null_count > 0:
            con.close()
            raise ValueError(
                f"[VALIDATE] ERREUR : {null_count} valeur(s) NULL détectée(s) "
                f"dans la colonne '{col}'."
            )

    print("[VALIDATE] ✅ Aucune valeur NULL détectée sur les champs essentiels.")
    con.close()
    print("[VALIDATE] Validation terminée avec succès.")


if __name__ == "__main__":
    validate()
