"""Pipeline ELT para extracción, transformación y carga de datos financieros.

Este script de Google Cloud Functions extrae información financiera y de
residentes desde un archivo Excel (o tipo Google Sheets) en Google Drive, limpia y transforma los
datos mediante Pandas, y los carga en Google BigQuery utilizando una
arquitectura Medallion (Capa Elemental y Capa Master) optimizada para el entorno de Looker Studio.
"""

import os
import io
import logging
import re
import pandas as pd
import functions_framework
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# ==========================================
# 1. IDs y Configuración (Variables de Entorno)
# ==========================================
PROJECT_ID = os.environ.get('ID proyecto', 'Tu ID de proyecto')
DRIVE_FILE_ID = os.environ.get('ID archivo drive', 'tu ID del Excel') 
DATASET_ID = os.environ.get('ID del Dataset', 'Tu Id del Dataset')
TOKEN_MAESTRO = os.environ.get('Token secreto', 'Tu Token secreto')  

# Selección de rango de tablas a extraer y sus configuraciones
TABLAS_A_EXTRAER = {
    "cuotas_2024_2025": {"tipo": "cuotas", "hoja": "BASE DE DATOS", "rango_cols": "A:O", "saltar_filas": 1, "num_filas": 15},
    "estado_caja_2024_2025": {"tipo": "caja", "hoja": "BASE DE DATOS", "rango_cols": "A:O", "saltar_filas": 18, "num_filas": 4},
    "gastos_2024_2025": {"tipo": "gastos", "hoja": "BASE DE DATOS", "rango_cols": "A:O", "saltar_filas": 24, "num_filas": 24},
    "registro_residentes": {"tipo": "residentes", "hoja": "BASE DE DATOS", "rango_cols": "C:E", "saltar_filas": 49, "num_filas": 15},
    "deuda_actual": {"tipo": "deuda", "hoja": "BASE DE DATOS", "rango_cols": "G:K", "saltar_filas": 49, "num_filas": 15},
    "cuotas_2026": {"tipo": "cuotas", "hoja": "BASE DE DATOS", "rango_cols": "R:AD", "saltar_filas": 1, "num_filas": 15},
    "estado_caja_2026": {"tipo": "caja", "hoja": "BASE DE DATOS", "rango_cols": "R:AD", "saltar_filas": 18, "num_filas": 4},
    "gastos_2026": {"tipo": "gastos", "hoja": "BASE DE DATOS", "rango_cols": "Q:AD", "saltar_filas": 24, "num_filas": 23}
}

# ==========================================
# 2. Extracción de Datos
# ==========================================
def obtener_excel_desde_drive(credenciales):
    """Descarga el archivo Excel desde Google Drive hacia la memoria temporal.

    Args:
        credenciales (google.oauth2.service_account.Credentials): 
            Credenciales de autenticación para las APIs de Google.

    Returns:
        io.BytesIO: Buffer de memoria que contiene los datos binarios del archivo Excel.
    """
    servicio = build('drive', 'v3', credentials=credenciales)
    request = servicio.files().get_media(fileId=DRIVE_FILE_ID)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer

# ==========================================
# 3. Módulo de Transformación (ELT y BI)
# ==========================================
def limpiar_nombres(df):
    """Normaliza y limpia los nombres de las columnas del DataFrame.

    Convierte formatos de fecha en textos estandarizados (ej. 'ene_2024'),
    elimina caracteres especiales, convierte todo a minúsculas y maneja
    nombres duplicados o vacíos.

    Args:
        df (pandas.DataFrame): El DataFrame con los nombres de columnas sin procesar.

    Returns:
        pandas.DataFrame: El DataFrame con los nombres de columnas estandarizados.
    """
    columnas_limpias = []
    meses = {1:'ene', 2:'feb', 3:'mar', 4:'abr', 5:'may', 6:'jun', 
             7:'jul', 8:'ago', 9:'sep', 10:'oct', 11:'nov', 12:'dic'}
             
    for i, col in enumerate(df.columns):
        nombre = str(col).strip()
        match_fecha = re.search(r'^(\d{4})[-_](\d{2})[-_](\d{2})', nombre)
        
        if match_fecha:
            year = match_fecha.group(1)
            month = int(match_fecha.group(2))
            nombre = f"{meses.get(month, 'mes')}_{year}"
        else:
            nombre = re.sub(r'[^a-zA-Z0-9_]', '_', nombre)
            nombre = re.sub(r'_+', '_', nombre).strip('_')
            if not nombre or nombre.lower() == "nan":
                nombre = f"columna_vacia_{i}"
            if nombre[0].isdigit():
                nombre = "col_" + nombre
                
        nombre = nombre.lower()
        if nombre in columnas_limpias:
            nombre = f"{nombre}_{i}"
        columnas_limpias.append(nombre)
        
    df.columns = columnas_limpias
    return df

def limpiar_datos(df):
    """Limpia el contenido del DataFrame manejando valores nulos y tipos de datos.

    Reemplaza celdas con guiones por NaN, intenta convertir las columnas a
    tipos numéricos (rellenando los nulos con 0.0) y convierte las columnas
    restantes a formato texto.

    Args:
        df (pandas.DataFrame): DataFrame con los datos en crudo.

    Returns:
        pandas.DataFrame: DataFrame con los tipos de datos corregidos.
    """
    df = df.replace(r'^\s*[-]*\s*$', float('nan'), regex=True)
    for col in df.columns:
        temp_col = pd.to_numeric(df[col], errors='ignore')
        if pd.api.types.is_numeric_dtype(temp_col):
            df[col] = temp_col.astype(float).fillna(0.0)
        else:
            df[col] = df[col].fillna("").astype(str)
    return df

def estructurar_para_looker_studio(df, tipo_tabla):
    """Transforma el DataFrame al formato tabular ('unpivoted') para Looker Studio.

    Si la tabla es de tipo 'cuotas', 'caja' o 'gastos', aplica una operación
    melt para convertir las columnas de meses en filas con formato fecha y monto.

    Args:
        df (pandas.DataFrame): DataFrame original en formato ancho (wide).
        tipo_tabla (str): Clasificación de la tabla (ej. 'cuotas', 'residentes').

    Returns:
        pandas.DataFrame: DataFrame estructurado en formato largo (long) si aplica,
            o el DataFrame original intacto para otros tipos de tablas.
    """
    if tipo_tabla in ['cuotas', 'caja', 'gastos']:
        id_col_original = df.columns[0]
        nombres_id = {'cuotas': 'departamento', 'caja': 'item', 'gastos': 'concepto'}
        nuevo_id = nombres_id[tipo_tabla]
        
        df = df.rename(columns={id_col_original: nuevo_id})
        meses_cols = [c for c in df.columns if c != nuevo_id]
        
        df_melted = df.melt(id_vars=[nuevo_id], value_vars=meses_cols, var_name='mes_anio', value_name='monto')
        
        meses_a_num = {'ene':'01', 'feb':'02', 'mar':'03', 'abr':'04', 'may':'05', 'jun':'06', 
                       'jul':'07', 'ago':'08', 'sep':'09', 'oct':'10', 'nov':'11', 'dic':'12'}
        
        def parse_date(ma):
            try:
                partes = ma.split('_')
                return f"{partes[1]}-{meses_a_num.get(partes[0], '01')}-01"
            except:
                return None
                
        df_melted['fecha'] = df_melted['mes_anio'].apply(parse_date)
        df_melted['fecha'] = pd.to_datetime(df_melted['fecha'], errors='coerce')
        df_melted = df_melted.dropna(subset=['fecha'])
        df_melted['monto'] = pd.to_numeric(df_melted['monto'], errors='coerce').fillna(0.0)
        
        return df_melted[[nuevo_id, 'fecha', 'monto']]
    else:
        return df

# ==========================================
# 4. Módulo Controlador (Entry Point)
# ==========================================
@functions_framework.http
def actualizar_base_datos(request):
    """Orquesta la ejecución del pipeline y responde a la petición HTTP.

    Valida un token de seguridad, descarga el Excel, ejecuta las transformaciones
    por cada tabla configurada y carga los resultados en las tablas de BigQuery.

    Args:
        request (flask.Request): Objeto de solicitud HTTP.

    Returns:
        tuple: Un string con el mensaje de respuesta y un int con el código HTTP.
    """
    try:
        token_recibido = request.args.get('token')
        if token_recibido != TOKEN_MAESTRO:
            return "Acceso Denegado.", 403
            
        creds = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/drive.readonly']
        )
        
        buffer = obtener_excel_desde_drive(creds)
        client = bigquery.Client(credentials=creds, project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        xls = pd.ExcelFile(buffer, engine='openpyxl')

        tablas_maestras = {
            "master_cuotas": [],
            "master_caja": [],
            "master_gastos": [],
            "master_residentes": [],
            "master_deuda": []
        }

        for nombre_tabla, config in TABLAS_A_EXTRAER.items():
            df = pd.read_excel(xls, sheet_name=config["hoja"], usecols=config["rango_cols"], skiprows=config["saltar_filas"], nrows=config["num_filas"], header=None)
            if df.empty: continue
            
            df.columns = df.values[0]
            df = df[1:].reset_index(drop=True)
            df = limpiar_nombres(df)
            df = limpiar_datos(df)
            
            # Guardado Capa Elemental (cruda y auditable)
            table_id_elemental = f"{PROJECT_ID}.{DATASET_ID}.{nombre_tabla}"
            client.delete_table(table_id_elemental, not_found_ok=True)
            client.load_table_from_dataframe(df, table_id_elemental, job_config=job_config).result()
            
            # Preparación Capa Master
            df_looker = estructurar_para_looker_studio(df.copy(), config["tipo"])
            llave_master = f"master_{config['tipo']}"
            tablas_maestras[llave_master].append(df_looker)

        # Guardado Capa Master (lista para usarse en Looker Studio)
        for nombre_master, lista_dfs in tablas_maestras.items():
            if lista_dfs:
                df_final = pd.concat(lista_dfs, ignore_index=True)
                table_id_master = f"{PROJECT_ID}.{DATASET_ID}.{nombre_master}"
                client.delete_table(table_id_master, not_found_ok=True)
                client.load_table_from_dataframe(df_final, table_id_master, job_config=job_config).result()
            
        return "Éxito absoluto: Tablas elementales y maestras actualizadas sin errores.", 200
        
    except Exception as e:
        return f"Error al procesar: {str(e)}", 500
