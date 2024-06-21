import json
import os
import sqlite3

import pandas as pd
import yaml
from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential
from termcolor import colored

from utils.utils import get_all, load_file

load_dotenv()

GPT_MODEL = "gpt-4o"
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
                        "description": "Define una métrica única para calcular o seleccionar, directamente de una columna existente o mediante una operación simple sobre columnas existentes. Ejemplos válidos incluyen 'POBTOT' o 'TVIVHAB * 1.2'. SOLO usar una métrica. La consulta final se formará como 'SELECT CLAVE_LOTE as clave, ({metric}) As value FROM predios WHERE {query}'.",
                    },
                    "condition": {
                        "type": "string",
                        "description": "Parte de condición para filtrar filas, basada en criterios definidos por el usuario y considerando solo la métrica definida. Ejemplos de condiciones válidas incluyen 'underutilized_ratio > 0.5' o 'POBTOT > 1000'. Esencial para enfocar el análisis en puntos de datos relevantes.",
                    },
                },
                "required": ["metric"],
            },
        },
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
        results = (
            f"Error: function {message.tool_calls[0].function.name} does not exist"
        )
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
            print(
                colored(
                    f"system: {message['content']}\n", role_to_color[message["role"]]
                )
            )
        elif message["role"] == "user":
            print(
                colored(f"user: {message['content']}\n", role_to_color[message["role"]])
            )
        elif message["role"] == "assistant" and message.get("function_call"):
            print(
                colored(
                    f"assistant: {message['function_call']}\n",
                    role_to_color[message["role"]],
                )
            )
        elif message["role"] == "assistant" and not message.get("function_call"):
            print(
                colored(
                    f"assistant: {message['content']}\n", role_to_color[message["role"]]
                )
            )
        elif message["role"] == "function":
            print(
                colored(
                    f"function ({message['name']}): {message['content']}\n",
                    role_to_color[message["role"]],
                )
            )


PROMPT = """
Como analista de datos especializado en desarrollo urbano, vivienda y análisis demográfico, tu rol es interpretar conjuntos de datos complejos y asistir en la elaboración de consultas SQL perspicaces. Utilizas extensos datos recolectados de INEGI para demografía, DENUE para establecimientos, y análisis detallados de uso de tierra y accesibilidad. Tus consultas ayudan a descubrir patrones en la utilización de viviendas, el potencial de desarrollo y la accesibilidad a servicios, basándose en un entendimiento sofisticado de la dinámica urbana.

Enfoque para Construir Consultas SQL:

Entendimiento del Requerimiento de Análisis: Primero, aclarar el objetivo analítico del usuario, como identificar lotes con potencial de desarrollo o evaluar el impacto de servicios cercanos en los valores de las viviendas.

Elaboración de la Consulta: Utilizar métodos SQL para construir consultas. Para filtrar datos, se usa la estructura SELECT CLAVE_LOTE AS clave, <metric> AS valor FROM predios WHERE <condición>;, donde <condición> involucra columnas y operaciones específicas (por ejemplo, POBTOT > 1000 para filtrar por población total mayor a 1000) y <metric> es la métrica específica a seleccionar o calcular, siendo una columna existente o una operación simple sobre columnas existentes.

Columnas Detalladas para la Construcción de Consultas:
{table_columns}
"""


def load_messages(folder: str):
    global PROMPT, MESSAGES
    df_lots = get_all(f"""SELECT * FROM lots""")
    columns = [
        x
        for x in df_lots.columns.to_list()
        if x not in ["geometry", "ID", "latitud", "longitud"]
    ]
    df_lots = (
        df_lots[columns]
        .describe()
        .transpose()[["mean", "min", "25%", "50%", "75%", "max"]]
    )
    with open("scripts/column_descriptions.yml", "r") as file:
        data = yaml.safe_load(file)
    df_lots["description"] = df_lots.index.map(data)
    PROMPT = PROMPT.format(table_columns=df_lots.to_markdown())
    MESSAGES = [{"role": "system", "content": PROMPT}]
    return PROMPT, MESSAGES


PROMPT, MESSAGES = load_messages("primavera")


def chat_response(user_message):
    MESSAGES.append({"role": "user", "content": user_message})
    chat_response = chat_completion_request(MESSAGES, tools=tools)
    print(chat_response)
    assistant_message = chat_response.choices[0].message
    # assistant_message.content = str(assistant_message.tool_calls[0].function)
    # messages.append({"role": assistant_message.role, "content": assistant_message.content})
    if assistant_message.tool_calls:
        print("Function call detected")
        results = execute_function_call(assistant_message)
        MESSAGES.append(
            {
                "role": "function",
                "tool_call_id": assistant_message.tool_calls[0].id,
                "name": assistant_message.tool_calls[0].function.name,
                "content": json.dumps(results),
            }
        )
        second_response = client.chat.completions.create(
            model=GPT_MODEL,
            messages=MESSAGES,
        )
        MESSAGES.append(
            {"role": "assistant", "content": second_response.choices[0].message.content}
        )
        return {
            "message": second_response.choices[0].message.content,
            "payload": results,
        }
    pretty_print_conversation(MESSAGES)
    return {"message": assistant_message.content, "payload": None}


if __name__ == "__main__":
    response = chat_response(
        "Could you create a query to identify lots with high utilization but still have significant room for development based on the current population density?"
    )
    print(response)
