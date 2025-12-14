import aiohttp      
import asyncio         
import os              
import zipfile         
import logging         
import polars as pl    
import json            
import fnmatch       
import shutil  
from modules import config
from modules.config import json_lock  
from modules import sql
from tqdm import tqdm                 


# Funções de criação de diretórios e JSON
def create_directory():
    os.makedirs(config.dir_zip, exist_ok=True)
    os.makedirs(config.dir_extraction, exist_ok=True)

def create_json():
    if not os.path.isfile(config.json_file_path):
        with open(config.json_file_path, "w") as write_json:
            json.dump(config.file_list, write_json)



# Função responsável pela leitura/escrita do CSV (e pela minha dor de cabeça)

def writecsv(path_zip, url, data, path_extraction, archive_name, content_type, main_loop):

    extraction_folder = os.path.join(path_extraction)
    csv_archive_path = os.path.join(config.dir_extraction, archive_name, archive_name.split('.zip')[0] + '.csv')

    # Define o schema do CSV, dando override no padrão do Polars

    pl_overrides = {
         "ride_id": pl.String, "rideable_type": pl.String,
         "started_at": pl.Datetime, "ended_at": pl.Datetime,
         "start_station_name": pl.String, "start_station_id": pl.String,
         "end_station_name": pl.String, "end_station_id": pl.String,
         "start_lat": pl.Float64, "start_lng": pl.Float64,
         "end_lat": pl.String, "end_lng": pl.String,
         "member_casual": pl.String
    }

    if content_type == 'application/zip':

        with open(path_zip, "wb") as f:
            f.write(data)

        with zipfile.ZipFile(path_zip, 'r') as z:
            z.extractall(path_extraction)

        # Leitura de CSV com Polars
        with pl.Config() as cfg:
            cfg.set_tbl_cols(-1)
            df = pl.read_csv(csv_archive_path, try_parse_dates=True, schema_overrides=pl_overrides)
            
            # Envia para o loop de evento principal e aguarda resultado
            future = asyncio.run_coroutine_threadsafe(sql.write_sql(df), main_loop)
            
            future.result() 

        # Deleta o diretório inteiro após insert no banco de dados

        if os.path.exists(extraction_folder):
            shutil.rmtree(extraction_folder)
            logging.info(f"Diretório deletado {extraction_folder}")

    else:
        logging.warning(f" {url} inválido")



# Função assíncrona para download de cada arquivo

async def download_data(session, url, downloaded_files):


    archive_name = url.split('/')[-1]
    path_zip = os.path.join(config.dir_zip, archive_name)
    path_extraction = os.path.join(config.dir_extraction, archive_name)
    loop = asyncio.get_running_loop()
    pattern = "*divvy-tripdata*"      



# Checa se arquivo já foi baixado ou se bate com o padrão de nome

    if archive_name in downloaded_files:
        logging.info(f"Arquivo {archive_name} já baixado")
        return

    if not fnmatch.fnmatch(archive_name, pattern):
        logging.info(f"Arquivo {archive_name} diferente do padrão")
        return 



    async with session.get(url) as response:
        if response.status != 200:
            raise ValueError(f"Erro HTTP {response.status} - {url}")

        content_type = response.headers.get('Content-Type')
        total = int(response.headers.get("Content-Length", 0))

        data_chunks = []
        progress_bar = tqdm(total=total, unit='B', unit_scale=True,
                            desc=f"Baixando {archive_name}", leave=False)

        # Baixa o arquivo em chunks
        async for chunk in response.content.iter_chunked(1024 * 64):
            data_chunks.append(chunk)
            progress_bar.update(len(chunk))

        progress_bar.close()
        data = b"".join(data_chunks)

        # Processa o arquivo em thread separada
        await loop.run_in_executor(
            config.executor,
            writecsv,
            path_zip,
            url,
            data,
            path_extraction,
            archive_name,
            content_type,
            loop
        )

    # Atualiza JSON com histórico de arquivos baixados com thread-safe lock

    with json_lock:
        downloaded_files.add(archive_name)
        with open(config.json_file_path, "w") as f:
            json.dump(list(downloaded_files), f, indent=4)


# Wrapper e função task

async def download_wrapper(session, url, pbar_global, downloaded_files):
    try:
        await download_data(session, url, downloaded_files)
    except Exception as e:
        logging.error(f"Erro ao processar {url}: {e}")
    finally:
        pbar_global.update(1)  # Atualiza barra global independentemente de erro

async def task(urls):


    # Thread lock
    with json_lock:
        create_json()
        with open(config.json_file_path, "r") as f:
            downloaded_files = set(json.load(f))

    pbar_global = tqdm(total=len(urls), desc="Progresso geral", unit="arquivo")

    async with aiohttp.ClientSession() as session:
        tasks = [download_wrapper(session, url, pbar_global, downloaded_files) for url in urls]
        await asyncio.gather(*tasks, return_exceptions=True)  # Executa todos os downloads paralelamente

    pbar_global.close()