INPUT_FOLDER=$1
OUTPUT_FOLDER=$2
COLUMN_ID=$3

python3 -m scripts.small2 "$INPUT_FOLDER" "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.inegi_denue "$INPUT_FOLDER" "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.landuse "$INPUT_FOLDER" "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.accesability "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 -m scripts.combine_all "$OUTPUT_FOLDER" "$COLUMN_ID"
python3 populate_db.py
