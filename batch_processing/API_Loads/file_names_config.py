from google.cloud import bigquery


schema =  [
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("properties", "STRING"),
    bigquery.SchemaField("geometry", "STRING"),
    bigquery.SchemaField("id", "STRING")
]

api_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=20230813&endtime=20230814"
table_id = "dt-sabah-sandbox-dev.load_api_data.Features_Earthquakes"

r_and_m_schema = schema =  [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("species", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("origin", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("image", "STRING"),
    bigquery.SchemaField("episode", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("created", "STRING")
]

# r_and_m_schema = [
#     bigquery.SchemaField("created", "STRING"),
#     bigquery.SchemaField("url", "STRING"),
#     bigquery.SchemaField("episode", "STRING"),
#     bigquery.SchemaField("image", "STRING"),
#     bigquery.SchemaField("location", "STRING"),
#     bigquery.SchemaField("origin", "STRING"),
#     bigquery.SchemaField("gender", "STRING"),
#     bigquery.SchemaField("type", "STRING"),
#     bigquery.SchemaField("species", "STRING"),
#     bigquery.SchemaField("status", "STRING"),
#     bigquery.SchemaField("name", "STRING"),
#     bigquery.SchemaField("id", "INTEGER")
# ]

r_and_m_table_id = "dt-sabah-sandbox-dev.load_api_data.Characters_R_and_M"
r_and_m_api_url = "https://rickandmortyapi.com/api/character"

poke_schema = schema =  [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("url", "STRING")
]
poke_table_id = "dt-sabah-sandbox-dev.load_api_data.Poke_api"
poke_api_url = "https://pokeapi.co/api/v2/pokemon"
poke_child_table_id = "dt-sabah-sandbox-dev.load_api_data.Poke_Encounters_api"

poke_child_schema = schema =  [
    bigquery.SchemaField("version_details", "STRING"),
    bigquery.SchemaField("location_area", "STRING")
]