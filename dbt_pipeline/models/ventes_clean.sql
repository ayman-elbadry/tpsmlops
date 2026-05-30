/*
    Modèle : ventes_clean
    Description :
        Nettoie les données brutes de la table 'ventes_raw'
        et calcule le chiffre d'affaires (CA) pour chaque ligne.
        CA = quantite * prix_unitaire
*/

SELECT
    date,
    produit,
    categorie,
    quantite,
    prix_unitaire,
    ville,
    quantite * prix_unitaire AS chiffre_affaires
FROM ventes_raw
