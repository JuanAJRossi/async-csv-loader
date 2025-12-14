# ASYNC-CSV-LOADER

É o exercicio de uma pipeline de ingestão de dados totalmente **assíncrona** que automatiza o processo de download de arquivos `.zip` de um bucket S3, processando e carregando pelo Polars os arquivos  `.csv` para um banco de dados PostgreSQL.

Feito para atender à necessidade de realizar o download de um grande volume de arquivos, evitando bloqueios de I/O, descompactando e processando os CSVs em thread executors, e carregando os dados em um banco de dados.

## Como instalar

Clone o repositório.

```
$ git clone https://github.com/JuanAJRossi/async-csv-loader
```

Configurar no **sql.py** dados do bd:

```python
DB_CONFIG = {
    'user': 'postgres',
    'password': 'test123',
    'database': 'example_db',
    'host': 'localhost'
}
```

Colocar a url do bucket dentro das variáveis no **config.py** :

```python
bucket_url = "https://divvy-tripdata.s3.amazonaws.com"
list_url = bucket_url + "/?list-type=2"
```


Monte a imagem.

```
docker build -t my-app-image
```




## Uso


Rode a imagem criada.

```
docker run -d -p 8080:80 my-app-image
```
