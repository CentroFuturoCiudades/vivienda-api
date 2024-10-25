from itertools import chain

BUILDING_CONFIDENCE = 0.65
PARK_TAGS = {
    'leisure': 'park',
    'landuse': 'recreation_ground'
}
EQUIPMENT_TAGS = {
    'amenity': [
        'place_of_worship',
        'school',
        'university'],
    'leisure': [
        'sports_centre',
        'pitch',
        'stadium'],
    'building': ['school'],
    'landuse': ['cemetery']
}
PARKING_FILTER = '["highway"~"service"]'
PARKING_TAGS = {
    'amenity': 'parking'
}
BUFFER_PARKING = 0.00002
GDFS_MAPPING = [
    {"name": 'unused', "color": 'blue'},
    {"name": 'green', "color": 'lightgreen'},
    {"name": 'parking', "color": 'gray'},
    {"name": 'building', "color": 'orange'},
]
WALK_SPEED = 1600 / 20 # 1,600 m / 20 min (80 meters per min)
WALK_RADIUS = WALK_SPEED * 60 # Maxium 1 hour of walking distance
AMENITIES_MAPPING = [
    # Salud
    {
        "name": "Hospital_general",
        "query_to": "amenity == 'Hospital general'",
        "pob_query": "pobtot",
        "amenity": "Hospital general",
        "attraction_query": "num_workers * 20",  # Each worker can attend to 20 patients per day
        "radius": 5000,
        "importance": 0.1,
    },
    {
        "name": "Consultorios_medicos",
        "query_to": "amenity == 'Consultorios médicos'",
        "pob_query": "pobtot",
        "amenity": "Consultorios médicos",
        "attraction_query": "num_workers * 2 * 8",  # Each worker can attend to 2 patients per hour, 8 hours a day
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Farmacia",
        "query_to": "amenity == 'Farmacia'",
        "pob_query": "pobtot",
        "amenity": "Farmacia",
        "attraction_query": "num_workers * 10 * 12",  # Each worker fills 10 prescriptions per hour (daily average), 12 hours a day
        "radius": 1000,
        "importance": 0.05,
    },
    # Recreativo
    {
        "name": "Parques_recreativos",
        "query_to": "amenity == 'Parques recreativos'",
        "pob_query": "pobtot",
        "amenity": "Parques recreativos",
        "attraction_query": "area / 30 * 2",  # 30 m² per visitor, 2 turnover cycles per day (morning and afternoon/evening)
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Clubs_deportivos_y_acondicionamiento_fisico",
        "query_to": "amenity == 'Clubs deportivos y de acondicionamiento físico'",
        "amenity": "Clubs deportivos y de acondicionamiento físico",
        "pob_query": "p_12a14 + p_15a17 + p_18a24 + p_25a59",
        "attraction_query": "area / 10 * 3",  # 10 m² per person, 3 turnover cycles per day (morning, afternoon, evening)
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Cine",
        "query_to": "amenity == 'Cine'",
        "pob_query": "pobtot",
        "amenity": "Cine",
        "attraction_query": "num_workers / 5 * 5 * 25",  # 5 workers per screen, 5 movies per day, 25 visitors per movie
        "radius": 5000,
        "importance": 0.03,
    },
    {
        "name": "Otros_servicios_recreativos",
        "query_to": "amenity == 'Otros servicios recreativos'",
        "pob_query": "p_12a14 + p_15a17 + p_18a24 + p_25a59",
        "amenity": "Otros servicios recreativos",
        "attraction_query": "num_workers * 200 / 7",  # Each worker can attend to 200 visitors per week, distributed across the week
        "radius": 3000,
        "importance": 0.02,
    },
    # Educación
    {
        "name": "Guarderia",
        "query_to": "amenity == 'Guarderia'",
        "pob_query": "p_0a2 + p_3a5",
        "amenity": "Guarderia",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Educacion_preescolar",
        "query_to": "amenity == 'Educación preescolar'",
        "pob_query": "p_3a5",
        "amenity": "Educación preescolar",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_primaria",
        "query_to": "amenity == 'Educación primaria'",
        "pob_query": "p_6a11",
        "amenity": "Educación primaria",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_secundaria",
        "query_to": "amenity == 'Educación secundaria'",
        "pob_query": "p_12a14",
        "amenity": "Educación secundaria",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_media_superior",
        "query_to": "amenity == 'Educación media superior'",
        "pob_query": "p_15a17",
        "amenity": "Educación media superior",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_superior",
        "query_to": "amenity == 'Educación superior'",
        "pob_query": "p_18a24",
        "amenity": "Educación superior",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    }
]
DENUE_TO_AMENITY_MAPPING = [
    # Salud
    {
        "name": "Consultorios médicos",
        "query": 'codigo_act.str.match("^621")',
    },
    {
        "name": "Farmacia",
        "query": 'codigo_act.str.match("^46411")',
    },
    # Recreativo
    {
        "name": "Cine",
        "query": 'codigo_act.str.match("^51213")',
    },
    {
        "name": "Otros servicios recreativos",
        "query": 'codigo_act.str.match("^7139")',
    },
    # Educación
    {
        "name": "Guarderia",
        "query": 'codigo_act.str.match("^6244")',
    },
    {
        "name": "Educación preescolar",
        "query": 'codigo_act.str.match("^61111")',
    },
]
AMENITIES_FILE_MAPPING = {
    # Salud
    'PRIMER NIVEL': 'Hospital general',
    'SEGUNDO NIVEL': 'Hospital general',
    'TERCER NIVEL': 'Hospital general',
    # Recreativo
    'PARQUE': 'Parques recreativos',
    'DEPORTE': 'Clubs deportivos y de acondicionamiento físico',
    # Educación
    'EDUCACION BASICA': 'Educación primaria',
    'BASICO': 'Educación primaria',
    'INTERMEDIO': 'Educación secundaria',
    'NIVEL MEDIO SUPERIOR': 'Educación media superior',
    'NIVEL SUPERIOR': 'Educación superior',
    'CAPILLA': 'Capilla',
    'COMEDOR': 'Comedor',
    # 'EDUCACION': 'Educación',
    # 'ABASTO': 'Abasto',
}
amenities_accessibility = {

}
MAX_ESTABLISHMENTS = 10
REGEX_PPL = r'([0-9]+) a ([0-9]+) personas'
DENUE = 'https://www.inegi.org.mx/contenidos/masiva/denue/denue_{0}_csv.zip'
CSV_DENUE = 'temp_data/conjunto_de_datos/denue_inegi_{0}_.csv'
URL_MZA_2010 = 'https://www.inegi.org.mx/contenidos/programas/ccpv/2010/datosabiertos/ageb_y_manzana/resageburb_{0}_2010_csv.zip'
URL_MZA_2020 = 'https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/ageb_manzana/ageb_mza_urbana_{0}_cpv2020_csv.zip'
CSV_PATH_MZA_2010 = 'temp_data/resultados_ageb_urbana_{0}_cpv2010/conjunto_de_datos/resultados_ageb_urbana_{0}_cpv2010.csv'
CSV_PATH_MZA_2020 = 'temp_data/ageb_mza_urbana_{0}_cpv2020/conjunto_de_datos/conjunto_de_datos_ageb_urbana_{0}_cpv2020.csv'
KEEP_COLUMNS = [
    'pobtot',
    'pobfem',
    'pobmas',
    'p_0a2',
    'p_0a2_f',
    'p_0a2_m',
    'p_3a5',
    'p_3a5_f',
    'p_3a5_m',
    'p_6a11',
    'p_6a11_f',
    'p_6a11_m',
    'p_12a14',
    'p_12a14_f',
    'p_12a14_m',
    'p_15a17',
    'p_15a17_f',
    'p_15a17_m',
    'p_18a24',
    'p_18a24_f',
    'p_18a24_m',
    'p_60ymas',
    'p_60ymas_f',
    'p_60ymas_m',
    'pea',
    'pe_inac',
    'pocupada',
    'pdesocup',
    'vivtot',
    'tvivhab',
    'tvivpar',
    'vivpar_hab',
    'vivpar_des',
    'ocupvivpar',
    'prom_ocup',
    'pro_ocup_c',
    'tvivparhab',
    'vph_1cuart',
    'vph_2cuart',
    'vph_3ymasc',
    'vph_pisodt',
    'vph_tinaco',
    'vph_excsa',
    'vph_drenaj',
    'vph_c_serv',
    'vph_refri',
    'vph_lavad',
    'vph_autom',
    'vph_tv',
    'vph_pc',
    'vph_telef',
    'vph_cel',
    'vph_inter',
    'vph_stvp',
    'vph_spmvpi',
    'vph_cvj',
    'pafil_ipriv',
    'graproes',
    'pcatolica',
    'pro_crieva',
    'potras_rel',
    'psin_relig',
]

MAPPING_SCORE_VARS = {
    'vph_pisodt': 1,
    'vph_tinaco': 1,
    'vph_excsa': 1,
    'vph_drenaj': 1,
    'vph_c_serv': 1,
    'vph_refri': 0.5,
    'vph_lavad': 0.7,
    'vph_autom': 1,
    'vph_tv': 0.5,
    'vph_pc': 0.5,
    'vph_telef': 0.2,
    'vph_cel': 0.2,
    'vph_inter': 0.5,
    'vph_stvp': 0.2,
    'vph_spmvpi': 0.2,
    'vph_cvj': 0.2,
}


BOUNDS_FILE = "bounds.fgb"
VEGETATION_FILE = "vegetation.fgb"
ESTABLISHMENTS_FILE = "establishments.fgb"
BUILDING_FILE = "buildings.fgb"
PROCESSED_BLOCKS_FILE = "processed_blocks.fgb"
LOTS_FILE = "lots.fgb"
ESTABLISHMENTS_LOTS_FILE = "establishments_lots.fgb"
LANDUSE_LOTS_FILE = "landuse_lots.fgb"
ACCESSIBILITY_BLOCKS_FILE = "accessibility_blocks.fgb"
AMENITIES_FILE = "amenities.fgb"
PEDESTRIAN_NETWORK_FILE = "pedestrian_network.h5"
ACCESSIBILITY_FILE = "accessibility_points.fgb"
ASSIGN_ESTABLISHMENTS_FILE = "assign_establishments.fgb"
ZONING_REGULATIONS_FILE = "zoning_regulations.json"
UTILIZATION_LOTS_FILE = "utilization_lots.fgb"