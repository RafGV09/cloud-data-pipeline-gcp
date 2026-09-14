# Automated ELT Pipeline for Real Estate Management

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Cloud](https://img.shields.io/badge/GCP-Cloud_Functions_%7C_BigQuery-1a73e8)
![BI](https://img.shields.io/badge/BI-Data_Studio-F4C20B)
![License](https://img.shields.io/badge/License-MIT-green)

# Autor: Rafael Guzman Viacava

## Impacto del Negocio
* **Reducción de trabajo manual:** Eliminación de horas operativas de cruce de datos en Excel.
* **Trazabilidad:** Semáforos dinámicos que se actualizan pasivamente al cierre de cada mes.
* **Seguridad:** Los datos viajan encriptados de extremo a extremo sin exponer credenciales locales.

## Visión General
Este proyecto implementa un pipeline de datos **ELT (Extract, Load, Transform)** 100% automatizado en Google Cloud Platform (GCP). Su objetivo es procesar, estructurar y disponibilizar la información financiera y administrativa de un complejo inmobiliario, transformando datos crudos en un panel de control interactivo para la toma de decisiones.

## Arquitectura de Datos

El flujo de datos opera de manera autónoma y se divide en 4 fases principales:

1. **Extracción (Google Drive API):** Lectura programada de una base de datos operativa alojada en Google Sheets.
2. **Transformación y Carga (Cloud Functions + Python):** 
   - Limpieza de datos (Data Cleansing) usando la librería `pandas`.
   - Normalización de estructuras matriciales (unpivoting) a series de tiempo.
   - Envío de datos bajo el paradigma Medallion Architecture (Capa Elemental y Capa Master).
3. **Almacenamiento (BigQuery):** Data Warehouse serverless que almacena el histórico y las tablas maestras estandarizadas.
4. **Visualización (Data Studio):** Dashboard gerencial que consume las vistas de BigQuery para mostrar KPIs financieros, mapas de calor de morosidad y flujos de caja.

## Tecnologías Utilizadas
* **Lenguaje:** Python 3.10+
* **Orquestación:** Google Cloud Scheduler (Cron jobs diarios)
* **Procesamiento:** Google Cloud Functions (HTTP Trigger)
* **Data Warehouse:** Google BigQuery
* **Business Intelligence:** Data Studio
* **Librerías principales:** `pandas`, `google-cloud-bigquery`, `google-api-python-client`

## Despliegue (Setup)
Para replicar esta arquitectura en un entorno propio:
1. Clonar el repositorio y configurar un proyecto en GCP.
2. Habilitar las APIs de **Google Drive**, **BigQuery** y **Cloud Functions**.
3. Configurar un `credentials.json` con permisos de cuenta de servicio.
4. Definir las variables de entorno en GCP (`PROJECT_ID`, `DRIVE_FILE_ID`, `DATASET_ID`, `SECRET_TOKEN`).
5. Desplegar el archivo `main.py` en Cloud Functions y programar su ejecución con Cloud Scheduler usando una solicitud HTTP `GET` autenticada.

