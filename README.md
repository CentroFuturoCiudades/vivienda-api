## Pre-requisites
### Setup Poetry
```sh
$ pipx install poetry
$ poetry install
```

### Process data
```sh
# OUTSIDE: Run notebook preprocess

$ echo "Gathering vegetation"
$ time poetry run python3 -m src.scripts.gather_vegetation <original-dir> "$tmp-dir"
$ echo "Gathering establishments"
$ time poetry run python3 -m src.scripts.gather_denue <original-dir> <tmp-dir> <state-code> <city-name>
$ echo "Gathering buildings"
$ time poetry run python3 -m src.scripts.gather_buildings <original-dir> <tmp-dir> <city-name>

$ echo "Processing lots"
$ time poetry run python3 -m src.scripts.process_lots <original-dir> <tmp-dir> <state-code> <state-name> <city-code>
$ echo "Assigning establishments"
$ time poetry run python3 -m src.scripts.assign_establishments <original-dir> <tmp-dir>
$ echo "Assigning landuse"
$ time poetry run python3 -m src.scripts.landuse <original-dir> <tmp-dir>

# OUTSIDE: Run notebook visits

echo "Calculating accessibility"
$ time poetry run python3 -m src.scripts.accessibility <original-dir> <tmp-dir>
$ echo "Calculating utilization"
$ time python3 -m src.scripts.utilization <original-dir> <tmp-dir>


cp <tmp_dir>/accessibility_blocks.fgb <final-dir>/blocks_complete.fgb
cp <tmp_dir>/utilization_lots.fgb <final-dir>/lots_complete.fgb
cp <tmp_dir>/accessibility_trips.csv <final-dir>/accessibility_trips.csv
cp <tmp_dir>/landuse_building.fgb <final-dir>/landuse_building.fgb
cp <tmp_dir>/amenities.fgb <final-dir>/amenities.fgb
cp <tmp_dir>/accessibility_points.fgb <final-dir>/accessibility_points.fgb

# OUTSIDE: Run notebook final
# OUTSIDE: Run notebook ideal buildings
```

## Setup local environment
```sh
$ psql -U uriels96 -d postgres -f init.sql
$ export POSTGRES_USER=<name>
$ export POSTGRES_PASSWORD=<password>
$ export POSTGRES_HOST=localhost
$ export POSTGRES_DB=reimaginaurbano
$ poetry run python3 -m src.scripts.populate_db -l "data/_primavera/final/lots.csv" -b "data/_primavera/final/blocks.csv" -a "data/_primavera/final/accessibility_trips.csv"
$ poetry run uvicorn src.main:app --reload --env-file .env.local
```

## Clean up Docker
Ensure you have cleaned up the docker containers and images before running the next command
```sh
docker stop $(docker ps -q)
docker rm $(docker ps -a -q)
docker rmi $(docker images -q)
docker volume rm $(docker volume ls -q)
docker system prune -a --volumes
```

## Setup Docker run app locally
```sh
$ docker-compose up
$ docker exec -it reimagina_urbano_app poetry run python -m src.scripts.populate_db -l "data/_primavera/lots.csv" -b "data/_primavera/blocks.csv" -a "data/_primavera/accessibility_trips.csv"
```

## Setup Docker run app in production
```sh
$ docker-compose up -d
$ docker exec -it reimagina_urbano_app poetry run python -m src.scripts.populate_db -l "https://reimaginaurbanostorage.blob.core.windows.net/primavera/lots.csv" -b "https://reimaginaurbanostorage.blob.core.windows.net/primavera/blocks.csv" -a "https://reimaginaurbanostorage.blob.core.windows.net/primavera/accessibility_trips.csv"
```

## Descarga de datos INEGI
### Descarga rapida
```sh
$ sh downloads.sh
```
### Marco Geoestadístico 2020 NL
- Pagina Datos: https://www.inegi.org.mx/app/biblioteca/ficha.html?upc=794551067314
- Descarga: https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/794551067314/19_nuevoleon.zip
### Principales resultados por AGEB y manzana urbana 2020 NL
- Pagina Datos: https://www.inegi.org.mx/programas/ccpv/2020/#microdatos
- Descarga: https://www.inegi.org.mx/contenidos/programas/ccpv/2020/microdatos/ageb_manzana/RESAGEBURB_19_2020_csv.zip
### Resultados sobre características del entorno urbano 2020 NL
- Pagina Datos: https://www.inegi.org.mx/programas/ccpv/2020/#microdatos
- Descarga: https://www.inegi.org.mx/contenidos/programas/ccpv/2020/microdatos/ceu/Censo2020_CEU_nl_csv.zip
### DENUE 2020 NL
- Pagina Datos: https://www.inegi.org.mx/app/descarga/default.html
- Descarga: https://www.inegi.org.mx/contenidos/masiva/denue/2020_04/denue_19_0420_csv.zip


## Descarga de GHSL builtup
curl -o ./data/builtup.zip https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_C_GLOBE_R2023A/GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10/V1-0/tiles/GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10_V1_0_R6_C9.zip
unzip -p ./data/builtup.zip GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10_V1_0_R6_C9.tif >./data/builtup.tif
rm ./data/builtup.zip

## Descarga de footprints
```sh
curl -o ./data/footprints.zip https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/867_buildings.csv.gz
```
Y de ahi tomar el archivo y ponerle el nombre de `buildings.csv`.

# Procesamiento de datos
## 
1. Crea un folder en `data` con el nombre de la zona que estas procesando.
2. Agrega un archivo `poligono.geojson` con el poligono de la zona de tu interes.
3. Agrega un archivo `predios.geojson` con los predios del poligono.
3. Corre el script de procesamiento de datos:
```sh
$ python3 -m scripts.small <folder>
```
Nota: Esto generará varios archivos en el folder que corresponden a datos del INEGI, DENUE, footprints de edificios, y bloques de la zona.
4. Corre el script de procesamiento de datos:
```sh
$ python3 -m scripts.landuse <folder> <output_folder>
```
Nota: Esto generará varios archivos en un nuevo folder que contiene la información geoespacial de los usos de suelo de la zona. También generará un archivo `predios.csv` que contiene toda la información combinada por predio.
5. Corre el script para crear una base de datos sobre los predios:
```sh
$ python3 populate_db.py <archivo_predio>
```
Nota: archivo_predio es el archivo `predios.csv` que se generó en el paso anterior. Esto creará una base de datos en `data/predios.db` con la información de los predios.

# IMPORTANT: Dependencies befor running the next section
- HDF5
Run the following command using homebrew
```sh
$ brew install hdf5
```

- UNIXOBDC
Run the following command using homebrew
```sh
$ brew install unixodbc
```

- Install microsoft OBDC 18 Drivers
```sh
$ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
$ brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
$ brew update
$ HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
$ odbcinst -u -d -n "ODBC Driver 18 for SQL Server"
```

# Ejecutar API
1. Instalar poetry si no se tiene ya agregado al sistema: 
```sh
$ pipx install poetry
```

2. Instalar las dependencias corriendo el comando:
```sh
$ poetry install
```

3. Inicializar el server con uvicorn ejecutando:
```sh
# Para utilizar los datos local
$ APP_ENV=local && poetry run uvicorn main:app --reload
# Para utilizar la base de datos en la nube
$ APP_ENV=dev && poetry run uvicorn main:app --reload
```

# Generate utility files
1. Run
```sh
sh ./run_all.sh data/primavera 25 "La Primavera" -v  
``` 