import asyncpg
from io import BytesIO


#Essa conexão é feita com um bd exemplo local. 

DB_CONFIG = {
    'user': 'postgres',
    'password': 'test123',
    'database': 'example_db',
    'host': 'localhost'
}


pool = None

# Pool de conexões
async def init_db_pool():
    global pool
    if pool is None:
            pool = await asyncpg.create_pool(
                min_size=5, 
                max_size=20, 
                **DB_CONFIG
            )
    return pool


async def close_db_pool():
    global pool
    if pool is not None:
        await pool.close()


# Função assíncrona para criação e insert dos dados no BD.
async def write_sql(df):
    global pool
    if pool is None:
        await init_db_pool()
        
    # Cria tabela caso já não exista uma.
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tbl_davytrip (
                id SERIAL PRIMARY KEY, 
                ride_id TEXT, 
                rideable_type TEXT, 
                started_at TIMESTAMP, 
                start_station_name TEXT,
                end_station_name TEXT,
                end_station_id TEXT,
                start_station_id TEXT,
                ended_at TIMESTAMP, 
                start_lat DOUBLE PRECISION, 
                start_lng DOUBLE PRECISION, 
                end_lat TEXT, 
                end_lng TEXT, 
                member_casual TEXT
            )
        ''')

        

        # Cria o DF do Polars na memória
        buffer = BytesIO()
        df.write_csv(buffer, include_header=False)
        buffer.seek(0)
        csv_data = buffer.getvalue()

        # Insert dos dados
        await conn.copy_to_table(
            table_name='tbl_davytrip',
            source=BytesIO(csv_data),
            columns=list(df.columns),
            format='csv'
        )
    

# Retorna uma query select do banco de dados.
async def select_sql():
    global pool
    if pool is None:
        await init_db_pool()
        
    async with pool.acquire() as conn:
        # Executa a query
        rows = await conn.fetch('''
            SELECT 
                rideable_type,
                EXTRACT(YEAR FROM ended_at) AS year,
                COUNT(*) AS quantity_by_type,
                SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) AS qt_member,
                SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS qt_casual
            FROM tbl_davytrip
            GROUP BY
                rideable_type,
                EXTRACT(YEAR FROM ended_at)
            ORDER BY
                rideable_type ASC,
                year ASC
        ''')

        for row in rows:
            print(dict(row))
