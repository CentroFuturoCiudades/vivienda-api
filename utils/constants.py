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
    {"name": 'park', "color": 'green'},
    {"name": 'equipment', "color": 'red'},
    {"name": 'building', "color": 'orange'},
]
WALK_RADIUS = 1609.34
WALK_SPEED = 1.2
AMENITIES_MAPPING = [
    {'column': 'proximity_small_park', 'type': 'park', 'query': 'park_area > 0.01'},
    {'column': 'proximity_big_park', 'type': 'park', 'query': 'park_area > 0.5'},
    {'column': 'proximity_supermercado', 'type': 'establishment', 'query': 'codigo_act.str.match("^462111")'},
    {'column': 'proximity_salud', 'type': 'establishment', 'query': 'codigo_act.str.match("^6211")'},
    {'column': 'proximity_educacion', 'type': 'establishment', 'query': 'codigo_act.str.match("^611")'},
    {'column': 'proximity_servicios', 'type': 'establishment', 'query': 'codigo_act.str.match("^81")'},
    {'column': 'proximity_age_diversity', 'type': 'home',
        'query': '(POBTOT > 0) and (P_15A17 + P_18A24 >= 0.2 * POBTOT) and (P_25A64 >= 0.2 * POBTOT) and (P_65MAS >= 0.2 * POBTOT)'},
]
PROXIMITY_MAPPING = {
    'proximity_big_park': 1,
    'proximity_small_park': 2,
    'proximity_salud': 2,
    'proximity_educacion': 1,
    'proximity_servicios': 5,
    'proximity_supermercado': 1,
    'proximity_age_diversity': 1,  # 10% of population is diverse
}
ACCESIBILITY_MAPPING = ['proximity_salud', 'proximity_educacion', 'proximity_servicios']
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
    'POBMAS',
    'POBFEM',
    'P_0A2',
    'P_3A5',
    'P_6A11',
    'P_15A17',
    'P_18A24',
    'POB0_14',
    'POB15_64',
    'POB65_MAS',
    'PEA',
    'PE_INAC',
    'POCUPADA',
    'PDESOCUP',
    'VIVTOT',
    'TVIVHAB',
    'TVIVPAR',
    'VIVPAR_HAB',
    'VIVPAR_DES',
    'VPH_AUTOM',
    'OCUPVIVPAR',
    'PROM_OCUP',
    'PRO_OCUP_C',
]