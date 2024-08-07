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
    {"name": 'amenity', "color": 'red'},
    {"name": 'parking', "color": 'gray'},
    {"name": 'building', "color": 'orange'},
]
WALK_RADIUS = 1609.34 * 2
WALK_SPEED = 1.2
AMENITIES_MAPPING = [
    {
        "name": "asistencial_social",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Asistencia social'",
        "radius": 2000,
        "importance": 0.1,
    },
    {
        "name": "Laboratorios_clinicos",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Laboratorios clínicos'",
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Otros_consultorios",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Otros consultorios'",
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Consultorios_medicos",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Consultorios médicos'",
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Hospital_general",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Hospital general'",
        "radius": 5000,
        "importance": 0.1,
    },
    {
        "name": "Hospitales_psiquiatricos",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Hospitales psiquiátricos'",
        "radius": 5000,
        "importance": 0.05,
    },
    {
        "name": "Hospitales_otras_especialidades",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Hospitales otras especialidades'",
        "radius": 5000,
        "importance": 0.05,
    },
    {
        "name": "Farmacia",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Farmacia'",
        "radius": 1000,
        "importance": 0.05,
    },
    {
        "name": "Clubs_deportivos_y_acondicionamiento_fisico",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Clubs deportivos y de acondicionamiento físico'",
        "radius": 2000,
        "importance": 0.05,
    },
    {
        "name": "Cine",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Cine'",
        "radius": 5000,
        "importance": 0.03,
    },
    {
        "name": "Otros_servicios_recreativos",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Otros Servicios recreativos'",
        "radius": 3000,
        "importance": 0.02,
    },
    # {
    #     "name": "Espectaculos_deportivos",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Espectáculos deportivos'",
    #     "radius": 5000,
    #     "importance": 0.02,
    # },
    {
        "name": "Parques_recreativos",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Parques recreativos'",
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Museos",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Museos'",
        "radius": 5000,
        "importance": 0.02,
    },
    {
        "name": "Biblioteca",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Biblioteca'",
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Guarderia",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Guarderia'",
        "radius": 3000,
        "importance": 0.05,
    },
    {
        "name": "Educacion_preescolar",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Educación Preescolar'",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_primaria",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Educación Primaria'",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_secundaria",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Educación Secundaria'",
        "radius": 3000,
        "importance": 0.15,
    },
    {
        "name": "Educacion_media_superior",
        "from": "home",
        "to": "amenity",
        "query_to": "amenity == 'Educación Media Superior'",
        "radius": 5000,
        "importance": 0.05,
    },
    # {
    #     "name": "Jardines_botanicos_y_zoologicos",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Jardines botánicos y zoológicos'",
    #     "radius": 5000,
    #     "importance": 0.02,
    # },
    # {
    #     "name": "Grutas_parques_naturales_o_patrimonio_cultural",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Grutas, parques naturales o patrimonio cultural'",
    #     "radius": 10000,
    #     "importance": 0.02,
    # },
    # {
    #     "name": "Espectaculos_artisticos_y_culturales",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Espectáculos artísticos y culturales'",
    #     "radius": 5000,
    #     "importance": 0.02,
    # },
    # {
    #     "name": "Educacion_secundaria_tecnica",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Educación Secundaria Técnica'",
    #     "radius": 3000,
    #     "importance": 0.05,
    # },
    # {
    #     "name": "Educacion_media_tecnica",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Educacion Media Técnica'",
    #     "radius": 3000,
    #     "importance": 0.05,
    # },
    # {
    #     "name": "Educacion_tecnica_superior",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Educación Técnica Superior'",
    #     "radius": 5000,
    #     "importance": 0.05,
    # },
    # {
    #     "name": "Educacion_superior",
    #     "from": "home",
    #     "to": "amenity",
    #     "query_to": "amenity == 'Educación Superior'",
    #     "radius": 5000,
    #     "importance": 0.05,
    # },
]
DENUE_TO_AMENITY_MAPPING = [
    {
        "name": "Asistencia social",
        "query": 'codigo_act.str.match("^623")',
    },
    {
        "name": "Laboratorios clínicos",
        "query": 'codigo_act.str.match("^6215")',
    },
    {
        "name": "Otros consultorios",
        "query": 'codigo_act.str.match("^6212")',
    },
    {
        "name": "Otros consultorios",
        "query": 'codigo_act.str.match("^6213")',
    },
    {
        "name": "Consultorios médicos",
        "query": 'codigo_act.str.match("^6211")',
    },
    {
        "name": "Hospital general",
        "query": 'codigo_act.str.match("^6221")',
    },
    {
        "name": "Hospitales psiquiátricos",
        "query": 'codigo_act.str.match("^6222")',
    },
    {
        "name": "Hospitales otras especialidades",
        "query": 'codigo_act.str.match("^6223")',
    },
    {
        "name": "Farmacia",
        "query": 'codigo_act.str.match("^46411")',
    },
    {
        "name": "Clubs deportivos y de acondicionamiento físico",
        "query": 'codigo_act.str.match("^71394")',
    },
    {
        "name": "Cine",
        "query": 'codigo_act.str.match("^51213")',
    },
    {
        "name": "Otros Servicios recreativos",
        "query": 'codigo_act.str.match("^7139")',
    },
    {
        "name": "Espectáculos deportivos",
        "query": 'codigo_act.str.match("^7112")',
    },
    {
        "name": "Parques recreativos",
        "query": 'codigo_act.str.match("^7131")',
    },
    {
        "name": "Jardines botánicos y zoológicos",
        "query": 'codigo_act.str.match("^71213")',
    },
    {
        "name": "Grutas, parques naturales o patrimonio cultural",
        "query": 'codigo_act.str.match("^7223")',
    },
    {
        "name": "Espectáculos artísticos y culturales",
        "query": 'codigo_act.str.match("^7111")',
    },
    {
        "name": "Museos",
        "query": 'codigo_act.str.match("^71211")',
    },
    {
        "name": "Biblioteca",
        "query": 'codigo_act.str.match("^51921")',
    },
    {
        "name": "Guarderia",
        "query": 'codigo_act.str.match("^6244")',
    },
    {
        "name": "Educación Preescolar",
        "query": 'codigo_act.str.match("^61111")',
    },
    {
        "name": "Educación Primaria",
        "query": 'codigo_act.str.match("^61112")',
    },
    {
        "name": "Educación Secundaria",
        "query": 'codigo_act.str.match("^61113")',
    },
    {
        "name": "Educación Secundaria Técnica",
        "query": 'codigo_act.str.match("^61114")',
    },
    {
        "name": "Educacion Media Técnica",
        "query": 'codigo_act.str.match("^61115")',
    },
    {
        "name": "Educación Media Superior",
        "query": 'codigo_act.str.match("^61116")',
    },
    {
        "name": "Educación Técnica Superior",
        "query": 'codigo_act.str.match("^61121")',
    },
    {
        "name": "Educación Superior",
        "query": 'codigo_act.str.match("^6113")',
    },
]
# AMENITIES_MAPPING = [
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Asistencia social",
#         "query": 'codigo_act.str.match("^623")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Laboratorios clínicos",
#         "query": 'codigo_act.str.match("^6215")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Otros consultorios",
#         "query": 'codigo_act.str.match("^6212")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Otros consultorios",
#         "query": 'codigo_act.str.match("^6213")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Consultorios médicos",
#         "query": 'codigo_act.str.match("^6211")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Hospital general",
#         "query": 'codigo_act.str.match("^6221")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Hospitales psiquiátricos",
#         "query": 'codigo_act.str.match("^6222")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Hospitales otras especialidades",
#         "query": 'codigo_act.str.match("^6223")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Farmacia",
#         "query": 'codigo_act.str.match("^46411")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Clubs deportivos y de acondicionamiento físico",
#         "query": 'codigo_act.str.match("^71394")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Cine",
#         "query": 'codigo_act.str.match("^51213")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Otros Servicios recreativos",
#         "query": 'codigo_act.str.match("^7139")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Espectáculos deportivos",
#         "query": 'codigo_act.str.match("^7112")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Parques recreativos",
#         "query": 'codigo_act.str.match("^7131")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Jardines botánicos y zoológicos",
#         "query": 'codigo_act.str.match("^71213")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Grutas, parques naturales o patrimonio cultural",
#         "query": 'codigo_act.str.match("^7223")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Espectáculos artísticos y culturales",
#         "query": 'codigo_act.str.match("^7111")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Museos",
#         "query": 'codigo_act.str.match("^71211")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Biblioteca",
#         "query": 'codigo_act.str.match("^51921")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Guarderia",
#         "query": 'codigo_act.str.match("^6244")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Preescolar",
#         "query": 'codigo_act.str.match("^61111")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Primaria",
#         "query": 'codigo_act.str.match("^61112")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Secundaria",
#         "query": 'codigo_act.str.match("^61113")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Secundaria Técnica",
#         "query": 'codigo_act.str.match("^61114")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educacion Media Técnica",
#         "query": 'codigo_act.str.match("^61115")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Media Superior",
#         "query": 'codigo_act.str.match("^61116")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Técnica Superior",
#         "query": 'codigo_act.str.match("^61121")',
#     },
#     {
#         "from": "home",
#         "to": "amenity",
#         "column": "equipment",
#         "type": "amenity",
#         "name": "Educación Superior",
#         "query": 'codigo_act.str.match("^6113")',
#     },
# ]
PROXIMITY_MAPPING = {
    'Educación Preescolar': 1,
    'Educación Primaria': 1,
    'Educación Secundaria': 1,
    'Educación Media Superior': 1,
    'Guarderia': 1,
    'Biblioteca': 1,
    'Hospital general': 1,
    'Laboratorios clínicos': 1,
    'Asistencia social': 1,
    'Consultorios médicos': 1,
    'Farmacia': 1,
    'park': 1,
    'Clubs deportivos y de acondicionamiento físico': 1,
    'Cine': 1,
    'Otros Servicios recreativos': 1,
    # 'Otros consultorios': 1,
    # 'Hospitales psiquiátricos': 1,
    # 'Hospitales otras especialidades': 1,
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
