#!/usr/bin/env python3
import pandas as pd
import requests
import json
import os
import subprocess
import sys
from datetime import datetime

def install_minio():
    try:
        import minio
        return True
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "minio"])
        return True

def get_config():
    """Get configuration from MinIO using DAG_KEY"""
    dag_key = os.environ.get('DAG_KEY', 'default')
    config_filename = f"cost-of-living-pipeline-{dag_key}.json"
    
    try:
        install_minio()
        from minio import Minio
        
        client = Minio(
            os.environ.get('MINIO_ENDPOINT', 'minio.minio-system.svc.cluster.local:9000'),
            access_key=os.environ.get('AWS_ACCESS_KEY_ID', 'minio'),
            secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123'),
            secure=False
        )
        
        bucket_name = 'customer-bucket'
        if client.bucket_exists(bucket_name):
            response = client.get_object(bucket_name, config_filename)
            config = json.loads(response.read().decode('utf-8'))
            print(f"✅ Config loaded: {config_filename}")
            print(f"   📊 Data URL: {config['data_fetcher']['data_url']}")
            return config
            
    except Exception as e:
        print(f"⚠️ Failed to load config {config_filename}: {e}")
    
    # Fallback default config
    return {
        "data_fetcher": {
            "data_url": "https://raw.githubusercontent.com/victorfonseca/elyra-poc/main/pipelines/COST_OF_LIVING/DATA/cost_of_living_v3.csv",
            "output_dir": "data",
            "csv_filename": "cost_of_living.csv",
            "json_filename": "cost_of_living.json",
            "request_timeout": 30,
            "max_retries": 3
        },
        "minio": {
            "bucket_name": "customer-bucket",
            "pipeline_prefix": "cost-of-living"
        }
    }

def upload_to_minio(local_file_path, bucket_name, object_name):
    """Upload file to MinIO"""
    from minio import Minio
    
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio.minio-system.svc.cluster.local:9000'),
        access_key=os.environ.get('AWS_ACCESS_KEY_ID', 'minio'),
        secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123'),
        secure=False
    )
    
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    client.fput_object(bucket_name, object_name, local_file_path)
    print(f"✅ Uploaded: {object_name}")

def fetch_data():
    """Fetch cost of living data using configuration"""
    config = get_config()
    fetcher_config = config['data_fetcher']
    minio_config = config['minio']
    
    data_url = fetcher_config['data_url']
    max_retries = fetcher_config['max_retries']
    timeout = fetcher_config['request_timeout']
    
    print(f"📡 Fetching data from: {data_url}")
    
    # Try to fetch data
    df = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(data_url, timeout=timeout)
            response.raise_for_status()
            df = pd.read_csv(data_url)
            print(f"✅ Data loaded successfully! Shape: {df.shape}")
            break
            
        except Exception as e:
            print(f"❌ Attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                # Fallback sample data
                df = pd.DataFrame({
                    'city': ['New York', 'London', 'Tokyo', 'Paris'],
                    'cost_index': [100, 95, 83, 87],
                    'rent_index': [100, 78, 51, 53],
                    'timestamp': datetime.now().strftime("%Y-%m-%d"),
                    'data_source': 'fallback_sample'
                })
                print(f"✅ Using fallback data: {len(df)} cities")
    
    # Save files locally
    output_dir = fetcher_config['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    
    csv_filename = fetcher_config['csv_filename']
    json_filename = fetcher_config['json_filename']
    
    csv_path = os.path.join(output_dir, csv_filename)
    json_path = os.path.join(output_dir, json_filename)
    
    df.to_csv(csv_path, index=False)
    with open(json_path, 'w') as f:
        f.write(df.to_json(orient='records', indent=2))
    
    print(f"💾 Files saved: {csv_path}, {json_path}")
    
    # Upload to MinIO
    bucket_name = minio_config['bucket_name']
    pipeline_prefix = minio_config['pipeline_prefix']
    dag_key = os.environ.get('DAG_KEY', 'default')
    
    csv_object = f"{pipeline_prefix}-{dag_key}/data/{csv_filename}"
    json_object = f"{pipeline_prefix}-{dag_key}/data/{json_filename}"
    
    try:
        upload_to_minio(csv_path, bucket_name, csv_object)
        upload_to_minio(json_path, bucket_name, json_object)
    except Exception as e:
        print(f"⚠️ MinIO upload failed: {e}")
    
    return {
        'status': 'success',
        'dag_key': dag_key,
        'csv_path': csv_path,
        'json_path': json_path,
        'minio_csv': f"{bucket_name}/{csv_object}",
        'minio_json': f"{bucket_name}/{json_object}",
        'row_count': len(df),
        'data_source': df.get('data_source', ['unknown'])[0] if len(df) > 0 else 'unknown'
    }

if __name__ == "__main__":
    print("🚀 Cost Data Fetcher with Config")
    print(f"   🔑 DAG Key: {os.environ.get('DAG_KEY', 'default')}")
    print("=" * 50)
    
    try:
        results = fetch_data()
        print("\n🎉 SUCCESS!")
        print(json.dumps(results, indent=2, default=str))
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()