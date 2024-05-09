INPUT_FOLDER=$1
OUTPUT_FOLDER=$2
COLUMN_ID=$3


# python3 -m scripts.green_area data/distritotec/final.gpkg -v
# python3 -m scripts.gather_denue data/distritotec/final.gpkg 19 -v
# python3 -m scripts.gather_buildings data/distritotec/final.gpkg -v
# python3 -m scripts.process_lots data/distritotec/final.gpkg data/distritotec/lots.gpkg 19 -v
# python3 -m scripts.assign_establishments data/distritotec/lots.gpkg data/distritotec/final.gpkg -v
# python3 -m scripts.accessibility data/distritotec/lots.gpkg data/distritotec/final.gpkg -v
# python3 -m scripts.landuse data/distritotec/lots.gpkg data/distritotec/final.gpkg -v
# python3 -m scripts.populate_db data/distritotec/lots.gpkg data/distritotec landuse -v


# python3 -m scripts.green_area data/la_primavera/processed.gpkg -v
# python3 -m scripts.gather_denue data/la_primavera/processed.gpkg 25 -v
# python3 -m scripts.gather_buildings data/la_primavera/processed.gpkg -v
# python3 -m scripts.process_lots data/la_primavera/processed.gpkg data/la_primavera/lots.gpkg 25 -v
# python3 -m scripts.assign_establishments data/la_primavera/lots.gpkg data/la_primavera/processed.gpkg -v
# python3 -m scripts.landuse data/la_primavera/lots.gpkg data/la_primavera/processed.gpkg -v
# python3 -m scripts.accessibility data/la_primavera/lots.gpkg data/la_primavera/processed.gpkg data/la_primavera -v
# python3 -m scripts.utilization -v
# python3 -m scripts.populate_db data/la_primavera/lots.gpkg data/la_primavera/predios.db final -v


python3 -m scripts.small2 "$INPUT_FOLDER" "$OUTPUT_FOLDER" "$COLUMN_ID"
# python3 -m scripts.small "$INPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.inegi_denue "$INPUT_FOLDER" "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.landuse "$INPUT_FOLDER" "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.accesability "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.combine_all "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.populate_db "$OUTPUT_FOLDER" "$COLUMN_ID"
