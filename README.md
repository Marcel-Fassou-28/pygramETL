# ETL et Analyse OLAP avec PygramETL, MySQL et JasperReport

## Description

Ce projet illustre un processus complet de **Business Intelligence**, depuis l’intégration et la transformation des données jusqu’à l’analyse multidimensionnelle. Il comprend :

- Extraction de données depuis des fichiers CSV/TXT (`customers.csv` et `states.txt`).  
- Transformation des données : calcul de `TotalSUM` et `AverageSUM`, ajout du nom des états, filtrage des clients sans état.  
- Chargement des données transformées dans la table MySQL `fact_customer`.  
- Définition d’un **cube OLAP Mondrian** pour analyser les données selon différentes dimensions et mesures.  
- Génération de rapports interactifs via **JasperReport**.

## Structure du projet
pygramETL/
├── data/
│ ├── customers.csv
│ └── states.txt
├── output/
│ └── transformed_customers.csv
├── etl/
│ └── main.py
|── customer_schema.xml
└── README.md

## Prérequis

- Python 3.x  
- MySQL  
- Packages Python :  
```bash
pip install -r requirements.txt

