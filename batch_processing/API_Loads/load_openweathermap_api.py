# import requests
# from google.cloud import bigquery

# client = bigquery.Client()

# # Fetch data from the PokeAPI
# api_url = "https://api.openweathermap.org/data/2.5/weather?q=London&appid=bc0c84f4a042a493510b26e23bcec635"  
# response = requests.get(api_url)
# weather_data = response.json()

# # Define destination table and schema
# destination_table_id = "dt-sabah-sandbox-dev.load_api_data.OpenWeatherMap_api"
# schema = schema = [
#     bigquery.SchemaField("coord", "RECORD",
#         fields=[
#             bigquery.SchemaField("lon", "FLOAT"),
#             bigquery.SchemaField("lat", "FLOAT")
#         ]
#     ),
#     bigquery.SchemaField("weather", "RECORD",
#         fields=[
#             bigquery.SchemaField("id", "INTEGER"),
#             bigquery.SchemaField("main", "STRING"),
#             bigquery.SchemaField("description", "STRING"),
#             bigquery.SchemaField("icon", "STRING")
#         ],
#         mode="REPEATED"
#     ),
#     bigquery.SchemaField("base", "STRING"),
#     bigquery.SchemaField("main", "RECORD",
#         fields=[
#             bigquery.SchemaField("temp", "FLOAT"),
#             bigquery.SchemaField("feels_like", "FLOAT"),
#             bigquery.SchemaField("temp_min", "FLOAT"),
#             bigquery.SchemaField("temp_max", "FLOAT"),
#             bigquery.SchemaField("pressure", "INTEGER"),
#             bigquery.SchemaField("humidity", "INTEGER")
#         ]
#     ),
#     bigquery.SchemaField("visibility", "INTEGER"),
#     bigquery.SchemaField("wind", "RECORD",
#         fields=[
#             bigquery.SchemaField("speed", "FLOAT"),
#             bigquery.SchemaField("deg", "FLOAT")
#         ]
#     ),
#     bigquery.SchemaField("clouds", "RECORD",
#         fields=[
#             bigquery.SchemaField("all", "INTEGER")
#         ]
#     ),
#     bigquery.SchemaField("dt", "INTEGER"),
#     bigquery.SchemaField("sys", "RECORD",
#         fields=[
#             bigquery.SchemaField("type", "INTEGER"),
#             bigquery.SchemaField("id", "INTEGER"),
#             bigquery.SchemaField("country", "STRING"),
#             bigquery.SchemaField("sunrise", "INTEGER"),
#             bigquery.SchemaField("sunset", "INTEGER")
#         ]
#     ),
#     bigquery.SchemaField("timezone", "INTEGER"),
#     bigquery.SchemaField("id", "INTEGER"),
#     bigquery.SchemaField("name", "STRING"),
#     bigquery.SchemaField("cod", "INTEGER")
# ]

# # Load data into BigQuery
# job_config = bigquery.LoadJobConfig(schema=schema)
# load_job = client.load_table_from_json(weather_data, destination_table_id, job_config=job_config)
# load_job.result() 

# print(f"Data loaded into {destination_table_id}.")

import requests
from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client()

# OpenWeatherMap API key and city name (replace with your own)
api_key = "bc0c84f4a042a493510b26e23bcec635"
city_name = "London"

# Fetch weather data from the OpenWeatherMap API
api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"
response = requests.get(api_url)
data = response.json()
# print(data)

# Define destination table and schema
destination_table_id = "dt-sabah-sandbox-dev.load_api_data.OpenWeatherMap_api" 

schema = [
    bigquery.SchemaField('coord', 'RECORD', mode='NULLABLE', fields=[
        bigquery.SchemaField('lon', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('lat', 'FLOAT', mode='NULLABLE')
    ]),
    bigquery.SchemaField('weather', 'RECORD', mode='REPEATED', fields=[
        bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('main', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('icon', 'STRING', mode='NULLABLE')
    ]),
    bigquery.SchemaField('base', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('main', 'RECORD', mode='NULLABLE', fields=[
        bigquery.SchemaField('temp', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('feels_like', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('temp_min', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('temp_max', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('pressure', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('humidity', 'INTEGER', mode='NULLABLE')
    ]),
    bigquery.SchemaField('visibility', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('wind', 'RECORD', mode='NULLABLE', fields=[
        bigquery.SchemaField('speed', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('deg', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('gust', 'FLOAT', mode='NULLABLE')
    ]),
    bigquery.SchemaField('clouds', 'RECORD', mode='NULLABLE', fields=[
        bigquery.SchemaField('all', 'INTEGER', mode='NULLABLE')
    ]),
    bigquery.SchemaField('dt', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('sys', 'RECORD', mode='NULLABLE', fields=[
        bigquery.SchemaField('type', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('sunrise', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('sunset', 'INTEGER', mode='NULLABLE')
    ]),
    bigquery.SchemaField('timezone', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('cod', 'INTEGER', mode='NULLABLE')
]

# Load data into BigQuery
job_config = bigquery.LoadJobConfig(schema=schema)
load_job = client.load_table_from_json([data], destination_table_id, job_config=job_config)
load_job.result()  # Wait for the job to complete

print(f"Weather data loaded into {destination_table_id}.")
