import requests
from datetime import datetime
import os

def download_nyc_taxi_files():
    output_dir = "nyc_taxi_machins"
    os.makedirs(output_dir, exist_ok=True)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    months = []
    
    for year in [2024, 2025]:
        end_month = 1 if year == 2025 else 12
        for month in range(1, end_month + 1):
            months.append(f"{year}-{month:02d}")
    
    print(f"Téléchargement de {len(months)} fichiers...\n")
    
    for i, month in enumerate(months, 1):
        file_name = f"yellow_tripdata_{month}.parquet"
        url = f"{base_url}{file_name}"
        file_path = os.path.join(output_dir, file_name)
        
        if os.path.exists(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"[{i}/{len(months)}] bim !")
            continue
        
        try:
            print(f"[{i}/{len(months)}] dl en cours de {file_name}...", end=" ")
            
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"bim !")
            
        except Exception as e:
            print(f" Erreur : {str(e)}")
    
    print(f"\n tout est dans : '{output_dir}'")

if __name__ == "__main__":
    download_nyc_taxi_files()