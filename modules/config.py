import os
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

# URLs do bucket S3
bucket_url = "https://divvy-tripdata.s3.amazonaws.com"
list_url = bucket_url + "/?list-type=2"

# Paths
dir_zip = os.path.join("data", "zip")
dir_extraction = os.path.join("data", "extraction")
json_file_path = "data/downloaded_files.json"

# Lista inicial de arquivos
file_list = []

# Lock para proteger escrita no JSON
json_lock = Lock()

# Executor para rodar funções CPU-bound
executor = ThreadPoolExecutor(max_workers=5)
