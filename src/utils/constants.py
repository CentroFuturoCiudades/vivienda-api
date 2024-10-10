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
        "pob_query": "POBTOT",
        "attraction_query": "num_workers * 20",  # Each worker can attend to 20 patients per day
        "radius": 5000,
        "importance": 0.1,
    },
    {
        "name": "Consultorios_medicos",
        "query_to": "amenity == 'Consultorios médicos'",
        "pob_query": "POBTOT",
        "attraction_query": "num_workers * 2 * 8",  # Each worker can attend to 2 patients per hour, 8 hours a day
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Farmacia",
        "query_to": "amenity == 'Farmacia'",
        "pob_query": "POBTOT",
        "attraction_query": "num_workers * 10 * 12",  # Each worker fills 10 prescriptions per hour (daily average), 12 hours a day
        "radius": 1000,
        "importance": 0.05,
    },
    # Recreativo
    {
        "name": "Parques_recreativos",
        "query_to": "amenity == 'Parques recreativos'",
        "pob_query": "POBTOT",
        "attraction_query": "area / 30 * 2",  # 30 m² per visitor, 2 turnover cycles per day (morning and afternoon/evening)
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Clubs_deportivos_y_acondicionamiento_fisico",
        "query_to": "amenity == 'Clubs deportivos y de acondicionamiento físico'",
        "pob_query": "P_12A14 + P_15A17 + P_18A24 + P_25A59",
        "attraction_query": "area / 10 * 3",  # 10 m² per person, 3 turnover cycles per day (morning, afternoon, evening)
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Cine",
        "query_to": "amenity == 'Cine'",
        "pob_query": "POBTOT",
        "attraction_query": "num_workers / 5 * 5 * 25",  # 5 workers per screen, 5 movies per day, 25 visitors per movie
        "radius": 5000,
        "importance": 0.03,
    },
    {
        "name": "Otros_servicios_recreativos",
        "query_to": "amenity == 'Otros servicios recreativos'",
        "pob_query": "P_12A14 + P_15A17 + P_18A24 + P_25A59",
        "attraction_query": "num_workers * 200 / 7",  # Each worker can attend to 200 visitors per week, distributed across the week
        "radius": 3000,
        "importance": 0.02,
    },
    # Educación
    {
        "name": "Guarderia",
        "query_to": "amenity == 'Guarderia'",
        "pob_query": "P_0A2 + P_3A5",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Educacion_preescolar",
        "query_to": "amenity == 'Educación preescolar'",
        "pob_query": "P_3A5",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_primaria",
        "query_to": "amenity == 'Educación primaria'",
        "pob_query": "P_6A11",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_secundaria",
        "query_to": "amenity == 'Educación secundaria'",
        "pob_query": "P_12A14",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_media_superior",
        "query_to": "amenity == 'Educación media superior'",
        "pob_query": "P_15A17",
        "attraction_query": "students + teachers",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_superior",
        "query_to": "amenity == 'Educación superior'",
        "pob_query": "P_18A24",
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
    'POBTOT',
    'POBFEM',
    'POBMAS',
    'P_0A2',
    'P_0A2_F',
    'P_0A2_M',
    'P_3A5',
    'P_3A5_F',
    'P_3A5_M',
    'P_6A11',
    'P_6A11_F',
    'P_6A11_M',
    'P_12A14',
    'P_12A14_F',
    'P_12A14_M',
    'P_15A17',
    'P_15A17_F',
    'P_15A17_M',
    'P_18A24',
    'P_18A24_F',
    'P_18A24_M',
    'P_60YMAS',
    'P_60YMAS_F',
    'P_60YMAS_M',
    'PEA',
    'PE_INAC',
    'POCUPADA',
    'PDESOCUP',
    'VIVTOT',
    'TVIVHAB',
    'TVIVPAR',
    'VIVPAR_HAB',
    'VIVPAR_DES',
    'OCUPVIVPAR',
    'PROM_OCUP',
    'PRO_OCUP_C',
    'TVIVPARHAB',
    'VPH_1CUART',
    'VPH_2CUART',
    'VPH_3YMASC',
    'VPH_PISODT',
    'VPH_TINACO',
    'VPH_EXCSA',
    'VPH_DRENAJ',
    'VPH_C_SERV',
    'VPH_REFRI',
    'VPH_LAVAD',
    'VPH_AUTOM',
    'VPH_TV',
    'VPH_PC',
    'VPH_TELEF',
    'VPH_CEL',
    'VPH_INTER',
    'VPH_STVP',
    'VPH_SPMVPI',
    'VPH_CVJ',
    'PAFIL_IPRIV',
    'GRAPROES',
    'PCATOLICA',
    'PRO_CRIEVA',
    'POTRAS_REL',
    'PSIN_RELIG',
]  # + list(chain(*[[f'P_{i*5}A{(i+1)*5-1}_F',
# f'P_{i*5}A{(i+1)*5-1}_M'] for i in range(0,
# 17)]))

MAPPING_SCORE_VARS = {
    'VPH_PISODT': 1,
    'VPH_TINACO': 1,
    'VPH_EXCSA': 1,
    'VPH_DRENAJ': 1,
    'VPH_C_SERV': 1,
    'VPH_REFRI': 0.5,
    'VPH_LAVAD': 0.7,
    'VPH_AUTOM': 1,
    'VPH_TV': 0.5,
    'VPH_PC': 0.5,
    'VPH_TELEF': 0.2,
    'VPH_CEL': 0.2,
    'VPH_INTER': 0.5,
    'VPH_STVP': 0.2,
    'VPH_SPMVPI': 0.2,
    'VPH_CVJ': 0.2,
}
