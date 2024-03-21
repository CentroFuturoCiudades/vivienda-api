process_data() {
  local url=$1
  local regex=$2
  local destination=$3
  local base_dir=$4

  # Create temporary directory
  mkdir -p "$base_dir/temp"
  # # Download the file
  curl -o "$base_dir/temp_zip.zip" "$url"
  # # Unzip the file
  unzip "$base_dir/temp_zip.zip" -d "$base_dir/temp"
  # Construct the full destination path
  local full_destination="$base_dir/$destination"

  # Check if destination is a directory or a file
  if [[ $destination == *.* ]]; then
    # Destination is a file
    local dir=$(dirname "$full_destination")
    mkdir -p "$dir"
    mv $base_dir/temp/$regex "$full_destination"
  else
    # Destination is a directory or ends with a slash
    mkdir -p "$full_destination"
    mv $base_dir/temp/$regex "$full_destination"
  fi

  # Clean up
  rm "$base_dir/temp_zip.zip"
  rm -r "$base_dir/temp"
}

# Entorno urbano por manzana
process_data https://www.inegi.org.mx/contenidos/programas/ccpv/2020/microdatos/ceu/Censo2020_CEU_nl_csv.zip TI_MANZANA_EU_19.csv entorno_manzanas.csv data/inegi
# Poblacion por manzana
process_data https://www.inegi.org.mx/contenidos/programas/ccpv/2020/microdatos/ageb_manzana/RESAGEBURB_19_2020_csv.zip RESAGEBURB_19CSV20.csv pob_manzanas.csv data/inegi
# Manzanas poligonos
process_data https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/794551067314/19_nuevoleon.zip conjunto_de_datos/19m.* geo_manzanas data/inegi
# DENUE 2020
process_data https://www.inegi.org.mx/contenidos/masiva/denue/2020_04/denue_19_0420_csv.zip conjunto_de_datos/denue_inegi_19_.csv denue.csv data/inegi
