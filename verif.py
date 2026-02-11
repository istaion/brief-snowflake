import pandas as pd
import requests
import io
from pathlib import Path

dossier = Path("nyc_taxi_machins")
liste_trucs = [str(path) for path in dossier.iterdir()]

for chemin in liste_trucs:
    df_sample = pd.read_parquet(chemin)

    print(f"✅ Fichier chargé : {len(df_sample):,} lignes\n")

    # Afficher les types de données réels
    print("📊 STRUCTURE DES DONNÉES")
    print("=" * 80)
    print(f"{'Colonne':<30} {'Type Pandas':<20} {'Type Snowflake suggéré':<25}")
    print("-" * 80)

    # Mapping Pandas → Snowflake
    type_mapping = {
        'int64': 'NUMBER(38,0)',
        'float64': 'FLOAT',
        'object': 'VARCHAR',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
        'datetime64[ns, UTC]': 'TIMESTAMP_NTZ',
        'bool': 'BOOLEAN'
    }

    for col in df_sample.columns:
        pandas_type = str(df_sample[col].dtype)
        snowflake_type = type_mapping.get(pandas_type, 'VARCHAR')
        print(f"{col:<30} {pandas_type:<20} {snowflake_type:<25}")

    print("\n📋 Aperçu des données :")
    print(df_sample.head(3))