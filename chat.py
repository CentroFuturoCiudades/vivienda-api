import json
from openai import OpenAI
from tenacity import retry, wait_random_exponential, stop_after_attempt
from termcolor import colored
from dotenv import load_dotenv
import os

load_dotenv()

GPT_MODEL = "gpt-4"
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
tools = [
    {
        "type": "function",
        "function": {
            "name": "build_sql_query",
            "description": "Esta función facilita la construcción dinámica y ejecución de consultas SQL adaptadas para el análisis de desarrollo urbano. Permite la creación de una métrica única a través de operaciones aritméticas en columnas existentes o la selección directa de una columna sin renombrar o combinar múltiples métricas. Ideal para extraer percepciones específicas de conjuntos de datos complejos.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "Define una métrica única para calcular o seleccionar, directamente de una columna existente o mediante una operación simple sobre columnas existentes. Ejemplos válidos incluyen 'POBTOT' o 'TVIVHAB * 1.2'. SOLO usar una métrica. La consulta final se formará como 'SELECT CLAVE_LOTE as clave, ({metric}) As value FROM predios WHERE {query}'."
                    },
                    "condition": {
                        "type": "string",
                        "description": "Parte de condición para filtrar filas, basada en criterios definidos por el usuario y considerando solo la métrica definida. Ejemplos de condiciones válidas incluyen 'utilization_ratio > 0.5' o 'POBTOT > 1000'. Esencial para enfocar el análisis en puntos de datos relevantes."
                    }
                },
                "required": ["metric"]
            }
        }
    }
]


def build_sql_query(data):
  print(data.get("metric"), data.get("condition"))
  return data


@retry(wait=wait_random_exponential(multiplier=1, max=40), stop=stop_after_attempt(3))
def chat_completion_request(messages, tools=None, model=GPT_MODEL):
  try:
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        tools=tools,
        tool_choice="auto",
        temperature=0.3,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
    )
    return response
  except Exception as e:
    print("Unable to generate ChatCompletion response")
    print(f"Exception: {e}")
    return e


def execute_function_call(message):
  if message.tool_calls[0].function.name == "build_sql_query":
    # Assuming arguments is already a dictionary
    arguments = message.tool_calls[0].function.arguments
    arguments = arguments.replace("'", '"')
    data = json.loads(arguments)
    results = build_sql_query(data)
  else:
    results = f"Error: function {message.tool_calls[0].function.name} does not exist"
  return results


def pretty_print_conversation(messages):
  role_to_color = {
      "system": "red",
      "user": "green",
      "assistant": "blue",
      "function": "magenta",
  }

  for message in messages:
    if message["role"] == "system":
      print(colored(f"system: {message['content']}\n", role_to_color[message["role"]]))
    elif message["role"] == "user":
      print(colored(f"user: {message['content']}\n", role_to_color[message["role"]]))
    elif message["role"] == "assistant" and message.get("function_call"):
      print(colored(f"assistant: {message['function_call']}\n", role_to_color[message["role"]]))
    elif message["role"] == "assistant" and not message.get("function_call"):
      print(colored(f"assistant: {message['content']}\n", role_to_color[message["role"]]))
    elif message["role"] == "function":
      print(colored(f"function ({message['name']}): {message['content']}\n", role_to_color[message["role"]]))


# PROMPT = """
# ChatGPT, as an expert data analyst specializing in urban development,
# housing, and demographic analysis, your role is to interpret complex
# datasets and assist in crafting insightful pandas DataFrame queries. You
# leverage extensive data collected from INEGI for demographics, DENUE for
# establishments, and detailed analyses of land use and accessibility.
# Your queries help uncover patterns in housing utilization, development
# potential, and accessibility to services based on a sophisticated
# understanding of urban dynamics.

# Approach to Constructing Queries:

# Analysis Requirement Understanding: First, clarify the user's analytical goal, such as identifying lots with potential for development or assessing the impact of nearby services on housing values.
# Query Crafting: Use pandas DataFrame methods to construct queries. For filtering data, .query('condition') is used, where 'condition' involves columns and operations (e.g., df.query('POBTOT > 1000')). For creating new metrics, operations on DataFrame columns directly (e.g., df['new_metric'] = df['POBTOT'] / df['TVIVHAB']) are performed.
# Detailed Data Columns for Query Construction:
# | Metric                        | Description                                                                                                                     | Use Case                                                                                     |
# |-------------------------------|---------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
# | `POBTOT`                      | Total Population within the lot's vicinity.                                                                                     | Analyzing demographic density and potential market size.                                    |
# | `TVIVHAB`                     | Total number of Inhabited Housing Units.                                                                                        | Gauging residential occupancy and housing demand.                                           |
# | `VIVPAR_DES`                  | Number of Uninhabited Private Dwellings.                                                                                        | Identifying potential housing stock or areas for development.                               |
# | `VIVTOT`                      | Total Housing Units, including both occupied and unoccupied.                                                                    | Understanding the full scope of housing availability.                                       |
# | `VPH_AUTOM`                   | Housing Units with access to a Car.                                                                                             | Assessing transportation amenities and potential need for parking infrastructure.          |
# | `building_area`               | Total Building Area within the lot.                                                                                             | Evaluating the extent of developed infrastructure versus potential for new development.    |
# | `building_ratio`              | Ratio of Building Area to total lot area.                                                                                       | Measuring development density and adherence to zoning regulations.                         |
# | `combined_score`              | A composite score evaluating lot desirability based on multiple factors.                                                        | Prioritizing lots for investment, development, or further detailed analysis.               |
# | `comercio`                    | Number of Commercial Establishments within a 15-minute walk.                                                                    | Analyzing commercial accessibility and the potential for retail development.               |
# | `continued_utilization_ratio` | Ratio indicating ongoing use of the lot, suggesting continuous or long-term utilization.                                        | Identifying lots with stable usage patterns for sustained investments.                     |
# | `educacion`                   | Number of Educational Services within a 15-minute walk.                                                                         | Assessing educational infrastructure and potential demand for family-oriented housing.     |
# | `green_area`                  | Area dedicated to green spaces within the lot.                                                                                  | Gauging environmental sustainability and recreational spaces.                              |
# | `green_ratio`                 | Ratio of Green Area to total lot area.                                                                                          | Measuring the balance between development and green spaces.                                |
# | `home_density`                | Density of Housing Units per unit of land area.                                                                                 | Understanding residential density for zoning and planning purposes.                        |
# | `num_establishments`          | Total number of commercial, health, and educational establishments in or near the lot.                                          | Evaluating the commercial and social infrastructure surrounding the lot.                   |
# | `num_properties`              | Number of Properties within the lot, indicating diversity of ownership or usage.                                                | Assessing property distribution for potential consolidation or development opportunities. |
# | `num_workers`                 | Number of Workers employed within or in proximity to the lot, indicative of economic activity.                                  | Analyzing economic vibrancy and employment opportunities in the area.                      |
# | `occupancy`                   | Occupancy Rate, reflecting the proportion of utilized space or dwellings.                                                       | Identifying under or over-utilized spaces for optimization.                                |
# | `occupancy_density`           | Density of Occupants per unit of land or dwelling area.                                                                         | Assessing the intensity of land use in terms of human occupancy.                           |
# | `equipment_area`              | Area dedicated to equipment and infrastructure within the lot.                                                                  | Evaluating the provision of essential services and infrastructure.                         |
# | `equipment_ratio`             | Ratio of Equipment Area to total lot area, indicating infrastructure development level.                                         | Measuring infrastructure development relative to the lot size.                             |
# | `parking_area`                | Area dedicated to parking within the lot.                                                                                       | Assessing the adequacy of parking facilities for residents and visitors.                   |
# | `parking_ratio`               | Ratio of Parking Area to total lot area, indicating the provision of parking relative to lot size.                              | Gauging the balance between parking provision and other land uses.                         |
# | `salud`                       | Number of Health Services within a 15-minute walk.                                                                              | Analyzing healthcare accessibility and infrastructure.                                     |
# | `services_nearby`             | General indicator of services within walking distance, encompassing salud, educacion, comercio, and other services.             | Evaluating overall accessibility to essential services.                                    |
# | `servicios`                   | General services like restaurants within a 15-minute walk.                                                                      | Understanding the provision of general services and public amenities.                      |
# | `total_score`                 | An overall score evaluating the lot based on all available metrics.                                                             | Comprehensive lot assessment for strategic planning and development.                       |

# Your expertise enables the translation of raw data into actionable insights, guiding urban planning and development decisions. By intelligently combining and manipulating these data points, craft queries that reflect nuanced user requests, ensuring a deep and actionable analysis.
# """

PROMPT = """
Como analista de datos especializado en desarrollo urbano, vivienda y análisis demográfico, tu rol es interpretar conjuntos de datos complejos y asistir en la elaboración de consultas SQL perspicaces. Utilizas extensos datos recolectados de INEGI para demografía, DENUE para establecimientos, y análisis detallados de uso de tierra y accesibilidad. Tus consultas ayudan a descubrir patrones en la utilización de viviendas, el potencial de desarrollo y la accesibilidad a servicios, basándose en un entendimiento sofisticado de la dinámica urbana.

Enfoque para Construir Consultas SQL:

Entendimiento del Requerimiento de Análisis: Primero, aclarar el objetivo analítico del usuario, como identificar lotes con potencial de desarrollo o evaluar el impacto de servicios cercanos en los valores de las viviendas.

Elaboración de la Consulta: Utilizar métodos SQL para construir consultas. Para filtrar datos, se usa la estructura SELECT CLAVE_LOTE AS clave, {metric} AS valor FROM predios WHERE {condición};, donde {condición} involucra columnas y operaciones específicas (por ejemplo, POBTOT > 1000 para filtrar por población total mayor a 1000) y {metric} es la métrica específica a seleccionar o calcular, siendo una columna existente o una operación simple sobre columnas existentes.

Columnas Detalladas para la Construcción de Consultas:
| Métrica             | Descripción                                                                   | Estadísticas (media, mínimo, 25%, 50%, 75%, máximo)         |
|---------------------|-------------------------------------------------------------------------------|-------------------------------------------------------|
| POBTOT              | Población total en la cercanía del lote. Util para análisis de densidad demográfica y tamaño de mercado potencial.| Media: 129.60, Min: 0.0, 25%: 25%: 56, 50%: 95, 75%: 154, Max: 1986|
| VIVPAR_DES          | Número de viviendas deshabitadas. Util para identificar potencial de vivienda o áreas para desarrollo.| Media: 3.64, Min: 0, 25%: 0, 50%: 0, 75%: 4, Max: 36|
| VIVTOT              | Unidades totales de vivienda, incluyendo ocupadas y no ocupadas. Util para entender la disponibilidad total de vivienda.| Media: 53.76, Min: 0, 25%: 23, 50%: 39, 75%: 60, Max: 774|
| building_ratio      | Proporción del área construida respecto al área total del lote. Util para medir la densidad de desarrollo y el cumplimiento de regulaciones de zonificación.| Media: 0.60, Min: 0, 25%: 0.47, 50%: 0.62, 75%: 0.75, Max: 1|
| green_ratio         | Proporción del área verde respecto al área total del lote. Util para medir el equilibrio entre desarrollo y espacios verdes.| Media: 0.02, Min: 0, 25%: 0, 50%: 0, 75%: 0, Max: 1|
| equipment_ratio     | Proporción del área de equipamiento respecto al área total del lote, indicando nivel de desarrollo de infraestructura. Util para medir el desarrollo de infraestructura relativo al tamaño del lote.| Media: 0.00, Min: 0, 25%: 0, 50%: 0, 75%: 0, Max: 1|
| parking_ratio       | Proporción del área de estacionamiento respecto al área total del lote, indicando la provisión de estacionamiento relativo al tamaño del lote. Util para medir el equilibrio entre la provisión de estacionamiento y otros usos de suelo.| Media: 0.01, Min: 0, 25%: 0, 50%: 0, 75%: 0, Max: 1|
| unused_ratio        | Proporción del área no utilizada respecto al área total del lote. Util para medir el potencial de desarrollo y la eficiencia del uso del suelo.| Media: 0.38, Min: 0, 25%: 0.24, 50%: 0.36, 75%: 0.50, Max: 1|
| wasteful_ratio      | Proporción del área desaprovechada respecto al área total del lote. Util para medir el potencial de desarrollo y la eficiencia del uso del suelo.| Media: 0.39, Min: 0, 25%: 0.24, 50%: 0.37, 75%: 0.50, Max: 1|
| num_workers         | Número de trabajadores empleados cerca del lote, indicador de actividad económica. Util para analizar la vitalidad económica y oportunidades de empleo en la zona.| Media: 2.43, Min: 0, 25%: 0, 50%: 0, 75%: 0, Max: 1096|
| num_establishments  | Número total de establecimientos comerciales, de salud y educativos cercanos al lote. Util para evaluar la infraestructura comercial y social que rodea el lote.| Media: 0.22, Min: 0, 25%: 0, 50%: 0, 75%: 0, Max: 77|
| educacion           | Número de servicios educativos a 15 minutos a pie. Util para evaluar infraestructura educativa y la demanda potencial de viviendas orientadas a la familia.| Media: 1.68, Min: 0, 25%: 0, 50%: 1, 75%: 2, Max: 16|
| salud               | Número de servicios de salud a 15 minutos a pie. Util para analizar la accesibilidad a la atención médica y la infraestructura.| Media: 3.18, Min: 0, 25%: 1, 50%: 3, 75%: 5, Max: 12|
| comercio            | Número de establecimientos comerciales a 15 minutos a pie. Util para analizar accesibilidad comercial y el potencial para desarrollo minorista.| Media: 17.30, Min: 0, 25%: 7, 50%: 14, 75%: 24, Max: 76|
| servicios           | Servicios generales como restaurantes a 15 minutos a pie. Util para entender la provisión de servicios generales y comodidades públicas.| Media: 9.58, Min: 0, 25%: 5, 50%: 9, 75%: 13, Max: 29|
| services_nearby     | Indicador general de servicios dentro de distancia a pie, incluyendo salud, educación, comercio y otros servicios. Util para evaluar la accesibilidad general a servicios esenciales.| Media: 42.16, Min: 0, 25%: 21, 50%: 37, 75%: 58, Max: 146|
| total_score         | Puntuación general que evalúa la accesibilidad del lote basada en todas la diversidad de servicios y establecimientos cercanos. Util para una evaluación completa del lote para planificación estratégica y desarrollo.| Media: 31.75, Min: 0, 25%: 16, 50%: 27, 75%: 44, Max: 106|
| combined_score      | Puntuación compuesta que evalúa la deseabilidad del lote basada en varios factores. Util para priorizar lotes para inversión, desarrollo o análisis detallado adicional.| Media: 0.24, Min: 0, 25%: 0.13, 50%: 0.20, 75%: 0.31, Max: 1|

Entiendo que buscas una adaptación completa de la tabla al español. Aquí tienes una versión ajustada:

Tu experiencia permite la traducción de datos brutos en insights accionables, guiando las decisiones de planificación y desarrollo urbano con consultas SQL inteligentemente combinadas y manipuladas, reflejando solicitudes de usuario matizadas para un análisis profundo y accionable.
"""

messages = []
messages.append({"role": "system", "content": PROMPT})


def chat_response(user_message):
  messages.append({"role": "user", "content": user_message})
  chat_response = chat_completion_request(
      messages, tools=tools
  )
  assistant_message = chat_response.choices[0].message
  # assistant_message.content = str(assistant_message.tool_calls[0].function)
  # messages.append({"role": assistant_message.role, "content": assistant_message.content})
  if assistant_message.tool_calls:
    print('Function call detected')
    results = execute_function_call(assistant_message)
    messages.append({"role": "function",
                    "tool_call_id": assistant_message.tool_calls[0].id,
                     "name": assistant_message.tool_calls[0].function.name,
                     "content": json.dumps(results)})
    second_response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=messages,
    )
    messages.append({"role": "assistant", "content": second_response.choices[0].message.content})
    return {"message": second_response.choices[0].message.content, "payload": results}
  pretty_print_conversation(messages)
  return {"message": assistant_message.content, "payload": None}


if __name__ == "__main__":
  chat_response("Could you create a query to identify lots with high utilization but still have significant room for development based on the current population density?")
