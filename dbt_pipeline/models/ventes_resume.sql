/*
    Modèle : ventes_resume
    Description :
        Agrège les données nettoyées par catégorie.
        Calcule la somme des quantités et du chiffre d'affaires (CA)
        pour chaque catégorie de produit.
*/

SELECT
    categorie,
    SUM(quantite)          AS total_quantite,
    SUM(chiffre_affaires)  AS total_chiffre_affaires
FROM {{ ref('ventes_clean') }}
GROUP BY categorie
