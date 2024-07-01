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
    {"name": 'unused',
"color": 'blue'},
    {"name": 'green',
"color": 'lightgreen'},
    {"name": 'parking',
"color": 'gray'},
    {"name": 'park',
"color": 'green'},
    {"name": 'equipment',
"color": 'red'},
    {"name": 'building',
"color": 'orange'},
]
WALK_RADIUS = 1609.34
WALK_SPEED = 1.2
AMENITIES_MAPPING = [
    {'column': 'small_park',
'type': 'park',
'query': 'park_area > 0.01'},
    {'column': 'big_park',
'type': 'park',
'query': 'park_area > 0.5'},
    {'column': 'supermercado',
'type': 'establishment',
'query': 'codigo_act.str.match("^462111")'},
    {'column': 'salud',
'type': 'establishment',
'query': 'codigo_act.str.match("^6211")'},
    {'column': 'educacion',
'type': 'establishment',
'query': 'codigo_act.str.match("^611")'},
    {'column': 'servicios',
'type': 'establishment',
'query': 'codigo_act.str.match("^81")'},
]
PROXIMITY_MAPPING = {
    'proximity_big_park': 1,
    'proximity_small_park': 2,
    'proximity_salud': 2,
    'proximity_educacion': 1,
    'proximity_servicios': 5,
    'proximity_supermercado': 1,
 # 10% of population is diverse
}
ACCESIBILITY_MAPPING = ['proximity_salud',
'proximity_educacion',
'proximity_servicios']
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
    'P_0A2_F',
    'P_0A2_M',
    'P_3A5_F',
    'P_3A5_M',
    'P_6A11_F',
    'P_6A11_M',
    'P_12A14_F',
    'P_12A14_M',
    'P_15A17_F',
    'P_15A17_M',
    'P_18A24_F',
    'P_18A24_M',
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
]# + list(chain(*[[f'P_{i*5}A{(i+1)*5-1}_F',
#f'P_{i*5}A{(i+1)*5-1}_M'] for i in range(0,
#17)]))

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