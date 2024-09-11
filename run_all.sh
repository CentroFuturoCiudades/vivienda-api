FOLDER=$1
STATE_CODE=$2
REGION=$3
VERBOSE_FLAG=$4

if [ "$VERBOSE_FLAG" == "-v" ]; then
    VERBOSE_OPTION="-v"
else
    VERBOSE_OPTION=""
fi

bounds_file="$FOLDER/original/bounds.geojson"
blocks_file="$FOLDER/original/blocks.geojson"
lots_file="$FOLDER/original/lots.fgb"
zoning_regulations_file="$FOLDER/original/zoning_regulations.json"
amenities_file="$FOLDER/original/EQUIPAMIENTOS/EQUIPAMIENTOS.shp"
altitude_tif_file="$FOLDER/original/Sinaloa_r15m.tif"

# Gather
vegetation_file="$FOLDER/final/vegetation.fgb"
establishments_file="$FOLDER/final/establishments.fgb"
buildings_file="$FOLDER/final/buildings.fgb"

mkdir -p "$FOLDER/tmp"
# Process
population_file="$FOLDER/tmp/population.fgb"
lots_establishments_file="$FOLDER/tmp/lots_establishments.fgb"
landuse_file="$FOLDER/tmp/landuse.fgb"
accessibility_file="$FOLDER/tmp/accessibility.fgb"

# Results
mkdir -p "$FOLDER/final"
pedestrian_network_file="$FOLDER/final/pedestrian_network.hd5"
accessibility_points_file="$FOLDER/final/accessibility_points.fgb"
landuse_other_file="$FOLDER/final/landuse_{}.fgb"
landuse_park_file="$FOLDER/final/landuse_amenity.fgb"
final_file="$FOLDER/final/lots.fgb"
db_file="$FOLDER/final/predios.db"


echo "Gathering vegetation"
time poetry run python3 -m scripts.gather_vegetation "$bounds_file" "$vegetation_file" $VERBOSE_OPTION
echo "Gathering establishments"
time poetry run python3 -m scripts.gather_denue "$bounds_file" "$establishments_file" $STATE_CODE $VERBOSE_OPTION
echo "Gathering buildings"
time poetry run python3 -m scripts.gather_buildings "$bounds_file" "$buildings_file" $VERBOSE_OPTION

echo "Processing lots"
time poetry run python3 -m scripts.process_lots "$bounds_file" "$blocks_file" "$lots_file" "$population_file" $STATE_CODE $VERBOSE_OPTION
echo "Assigning establishments"
time poetry run python3 -m scripts.assign_establishments "$population_file" "$establishments_file" "$lots_establishments_file" $VERBOSE_OPTION
echo "Assigning landuse"
time poetry run python3 -m scripts.landuse "$bounds_file" "$lots_establishments_file" "$buildings_file" "$establishments_file" "$amenities_file" "$vegetation_file" "$landuse_other_file" "$landuse_file" $VERBOSE_OPTION
echo "Calculating accessibility"
time poetry run python3 -m scripts.accessibility "$bounds_file" "$landuse_file" "$establishments_file" "$landuse_park_file" "$pedestrian_network_file" "$accessibility_points_file" "$accessibility_file" $VERBOSE_OPTION
echo "Calculating utilization"
time python3 -m scripts.utilization "$bounds_file" "$accessibility_file" "$lots_file" "$zoning_regulations_file" "$final_file" $VERBOSE_OPTION
echo "Populating database"
time poetry run python3 -m scripts.populate_db "$final_file" "$db_file" "$REGION" $VERBOSE_OPTION
echo "Calculate lots slope"
time poetry run python3 -m scripts "$bounds_file" "$altitude_tif_file" "$lots_file" "$db_file"