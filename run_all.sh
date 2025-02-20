FOLDER=$1
STATE_CODE=$2
VERBOSE_FLAG=$3

# TODO: Have csv file of regions with their codes and names
STATE_NAME='Sinaloa'
CITY_CODE='794551086179'
CITY_NAME='Culiacán'

# STATE_NAME='Nuevo_Leon'
# CITY_CODE='794551077313'
# CITY_NAME='Monterrey'

if [ "$VERBOSE_FLAG" == "-v" ]; then
    VERBOSE_OPTION="-v"
else
    VERBOSE_OPTION=""
fi

original_dir="$FOLDER/original"
tmp_dir="$FOLDER/tmp"
final_dir="$FOLDER/final"

mkdir -p $tmp_dir
mkdir -p "$FOLDER/final"

# OUTSIDE: Run notebook preprocess

# echo "Gathering vegetation"
# time poetry run python3 -m src.scripts.gather_vegetation "$original_dir" "$tmp_dir" $VERBOSE_OPTION
# echo "Gathering establishments"
# time poetry run python3 -m src.scripts.gather_denue "$original_dir" "$tmp_dir" $STATE_CODE $CITY_NAME $VERBOSE_OPTION
# echo "Gathering buildings"
# time poetry run python3 -m src.scripts.gather_buildings2 "$original_dir" "$tmp_dir" $CITY_NAME $VERBOSE_OPTION

# echo "Processing lots"
# time poetry run python3 -m src.scripts.process_lots "$original_dir" "$tmp_dir" $STATE_CODE $STATE_NAME $CITY_CODE $VERBOSE_OPTION
# echo "Assigning establishments"
# time poetry run python3 -m src.scripts.assign_establishments "$original_dir" "$tmp_dir" $VERBOSE_OPTION
# echo "Assigning landuse"
# time poetry run python3 -m src.scripts.landuse "$original_dir" "$tmp_dir" $VERBOSE_OPTION
# OUTSIDE: Run notebook visits
# echo "Calculating accessibility"
# time poetry run python3 -m src.scripts.accessibility "$original_dir" "$tmp_dir" $VERBOSE_OPTION
# echo "Calculating utilization"
# time poetry run python3 -m src.scripts.utilization "$original_dir" "$tmp_dir" $VERBOSE_OPTION

# cp "$tmp_dir/accessibility_blocks.fgb" "$final_dir/blocks_complete.fgb"
# cp "$tmp_dir/utilization_lots.fgb" "$final_dir/lots_complete.fgb"
# cp "$tmp_dir/accessibility_trips.csv" "$final_dir/accessibility_trips.csv"
# cp "$tmp_dir/landuse_building.fgb" "$final_dir/landuse_building.fgb"
# cp "$tmp_dir/amenities.fgb" "$final_dir/amenities.fgb"
# cp "$tmp_dir/accessibility_points.fgb" "$final_dir/accessibility_points.fgb"

# OUTSIDE: Run notebook slope
# OUTSIDE: Run notebook final
# OUTSIDE: Run notebook ideal buildings

# psql -h localhost -p 5432 -U uriels96 -d reimaginaurbano -f init.sql
# echo "Populate DB"
# time poetry run python -m src.scripts.populate_db -l "$final_dir/lots.csv" -b "$final_dir/blocks.csv" -a "$final_dir/accessibility_trips.csv"