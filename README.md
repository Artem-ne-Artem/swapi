# SWAPI

![This is an alt text.](/images/star_wars_logo.jpg "STAR WARS LOGO")

API «Звёздных войн», или «swapi» — это источник для всех данных канонической вселенной «Звёздных войн»!

### Используемые инструменты
* ETL - airflow:2.10.5
* DWH - postgres:14-alpine
* WEB connect to DWH - adminer:5.4.0
* BI - metabase:v0.56.9
* Инфраструктура - Docker

<!-- Смотреть в Metabase:  [Starships Metrics Dashboard](http://localhost:3000/public/dashboard/000607f9-6d26-4495-a0e9-93db833349f6) -->
<!-- [![Open in Metabase](https://img.shields.io/badge/Metabase-Open%20Dashboard-blue)](http://localhost:3000/public/dashboard/000607f9-6d26-4495-a0e9-93db833349f6) -->

![This is an alt text.](/images/swapi_dashboard.jpg "Dashboard")

### Схема процесса. Обновление данных во всех слоях происходит в формате full refresh.
![This is an alt text.](/images/swapi_process_schema.jpg "SWAPI process schema")


API Documentation https://swapi.dev/documentation#intro

API Shema: https://swapi.dev/api/<resource>/schema