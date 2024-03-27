FOLDER=$1
OUTPUT_FOLDER=$2

python3 -m scripts.small "$FOLDER"
python3 -m scripts.inegi_denue "$FOLDER" "$OUTPUT_FOLDER"
python3 -m scripts.landuse "$FOLDER" "$OUTPUT_FOLDER"
python3 -m scripts.accesability "$OUTPUT_FOLDER"
python3 -m scripts.combine_all "$OUTPUT_FOLDER"
python3 populate_db.py
