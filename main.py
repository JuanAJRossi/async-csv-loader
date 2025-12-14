import asyncio
import logging
import os
from modules import fetch_data
from modules import download_write
from modules import sql

# Configuração de logging

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)


async def run_pipeline():

    download_write.create_directory()
    
    
    await sql.init_db_pool() 


    xml = await fetch_data.fetch_object_list()
    urls = fetch_data.parse_s3_list(xml)

    logging.info(f" {len(urls)} Arquivos para download.")
        

    #Roda todo o processo assíncrono e multithread, iniciando pela task de download e escrita no banco de dados, depois o select e por fim fecha a pool de conexões.
    
    await download_write.task(urls)
    await sql.select_sql() #Existe absolutamente motivo nenhum pra esse select estar aqui agora.
    await sql.close_db_pool()


def main():

        asyncio.run(run_pipeline())



if __name__ == "__main__":
    main()