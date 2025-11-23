# SWAPI

![This is an alt text.](/images/star_wars_logo.jpg "STAR WARS LOGO")

API «Звёздных войн», или «swapi» — это источник для всех данных канонической вселенной «Звёздных войн»!

### Используемые инструменты
* airflow:2.10.5
* postgres:14-alpine
* adminer:5.4.0
* metabase:v0.56.9

### Схема процесса
![This is an alt text.](/images/swapi_process_schema.jpg "SWAPI process schema")

### Обновление данных во всех слоях происходит в формате full refresh.
* С помощью Airflow выгружаем данные AS IS из источника API в RAW слой Postgres. 
* Из RAW слоя Airflow трансформирует и загружает данные в слой STG Postgres.
* Из STG слоя Airflow формирует слой CDM Postgres.
* На базе CDM слоя MetaBase строит отчёт.

<!-- Смотреть в Metabase:  [Starships Metrics Dashboard](http://localhost:3000/public/dashboard/000607f9-6d26-4495-a0e9-93db833349f6) -->
[![Open in Metabase](https://img.shields.io/badge/Metabase-Open%20Dashboard-blue)](http://localhost:3000/public/dashboard/000607f9-6d26-4495-a0e9-93db833349f6)


Documentation https://swapi.dev/documentation#intro

Shema: https://swapi.dev/api/<resource>/schema
