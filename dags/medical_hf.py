from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import requests
import os

@task
def hf_medical_download():
    """Télécharge datasets médicaux publics Parquet"""
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    # Dataset 1: Termes médicaux wiki CC0 ✅ PUBLIC
    urls = {
        "wiki_medical_terms": "https://huggingface.co/datasets/gamino/wiki_medical_terms/resolve/main/data/train-00000-of-00001.parquet",
        "wikimedqa": "https://huggingface.co/datasets/sileod/wikimedqa/resolve/main/train-00000-of-00001.parquet"
    }
    
    for name, url in urls.items():
        print(f"📥 {name}...")
        r = requests.get(url)
        if r.status_code == 200:
            df = pd.read_parquet(r.content)
            df.to_parquet(f'/opt/airflow/data/{name}.parquet')
            print(f"✅ {name}: {len(df)} rows → data/")
        else:
            print(f"❌ {name}: HTTP {r.status_code}")
    
    return "Datasets médicaux OK !"

dag = DAG(
    'medical_hf_final',
    start_date=datetime(2026,1,17),
    schedule=None,
    catchup=False,
    tags=['medical', 'hf', 'parquet', 'public']
)

hf_medical_download()