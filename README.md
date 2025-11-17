# ETL – Data Mart de Ventas (AdventureWorks)

Este proyecto implementa una herramienta ETL en Python para construir un Data Mart de Ventas basado en la base transaccional AdventureWorks (SQL Server) y almacenado en PostgreSQL. El proceso realiza extracción, transformación, limpieza y carga incremental en tablas de staging y modelos dimensionales.

## 1. Requisitos

### Software necesario

* Python 3.10+
* SQL Server (AdventureWorks2019 o AdventureWorks2022)
* PostgreSQL 14+
* ODBC Driver 17+ para SQL Server

## 2. Instalación del entorno

Clonar el repositorio:

```bash
git clone <url_del_repositorio>
cd ETL
```

Crear un ambiente virtual:

```bash
python -m venv .venv
```

Activar (Windows):

```bash
.venv\Scripts\activate
```

Activar (Linux/Mac):

```bash
source .venv/bin/activate
```

Instalar dependencias:

```bash
pip install -r requirements.txt
```

## 3. Configurar conexiones

Editar `scripts/etl_main.py`:

### Conexión a SQL Server

```python
src_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=TU_SERVER;"
    "DATABASE=AdventureWorks2022;"
    "UID=tu_usuario;"
    "PWD=tu_password"
)
```

### Conexión a PostgreSQL

```python
dst_conn = psycopg2.connect(
    "host=localhost dbname=etl_db user=postgres password=tu_password"
)
```

## 4. Crear tablas en PostgreSQL

Abrir PostgreSQL:

```bash
psql -U postgres -d etl_db
```

Ejecutar los scripts DDL:

Tablas de staging:

```sql
\i sql/staging_tables.sql;
```

Dimensiones y hechos:

```sql
\i sql/object_creation.sql;
```

## 5. Ejecutar el proceso ETL

Desde la carpeta del proyecto:

```bash
cd scripts
python etl_main.py
```

El proceso realiza:

* Extracción desde SQL Server
* Normalización de datos (fechas, enteros, nulos)
* Exportación a CSV (carpeta `scripts/tmp/`)
* COPY hacia tablas de staging en PostgreSQL
* UPSERT de dimensiones
* Carga de hechos con granularidad final
* Registro de carga incremental en `last_run_timestamp.txt`

## 6. Estructura del proyecto

```
ETL/
│
├── .venv/
├── logs/
│   └── etl.log
│
├── scripts/
│   ├── etl_main.py
│   ├── tmp/
│   │   ├── stg_headers.csv
│   │   ├── stg_items.csv
│   │   ├── stg_customers.csv
│   │   ├── stg_products.csv
│   │   ├── stg_resellers.csv
│   │   └── stg_salespersons.csv
│
├── sql/
│   ├── staging_tables.sql
│   ├── object_creation.sql
│
└── README.md
```

## 7. Notas importantes

### Carga incremental

El archivo:

```
last_run_timestamp.txt
```

guarda la última fecha procesada.
Si deseas cargar todo de nuevo, elimínalo.

### CSV temporales

Se generan en cada ejecución en:

```
scripts/tmp/
```

### Manejo de NULLs

Todos los valores nulos o tipos inválidos se transforman antes del COPY para evitar errores como:

```
invalid input syntax for type bigint: "\N"
```

### Logs

El registro completo del proceso se guarda en:

```
logs/etl.log
```

## 8. Extensiones futuras

* Orquestación con Airflow
* Dockerización del pipeline
* Dashboards en Power BI/Tableau
* Automatización con cron o GitHub Actions
