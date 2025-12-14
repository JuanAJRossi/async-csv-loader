import aiohttp
import logging
import xml.etree.ElementTree as ET
from modules import config
from urllib.parse import urljoin



# Função assíncrona que busca a lista de objetos do bucket S3

async def fetch_object_list():
    async with aiohttp.ClientSession() as session:
        async with session.get(config.list_url) as response:
            if response.status != 200:
                logging.error(f"Erro ao acessar {config.list_url}: {response.status}")
                return [] 
            text = await response.text()
            return text 



# Função que faz parse do XML e extrai URLs

def parse_s3_list(xml_text):

    urls = []
    root = ET.fromstring(xml_text)

    for contents in root.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
        key = contents.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key")
        if key is not None:
            key_text = key.text  # Nome do arquivo
            if key_text.endswith(".zip"):
                # Monta a URL completa do arquivo no bucket
                full_url = urljoin(config.bucket_url + "/", key_text)
                urls.append(full_url)

    return urls 
