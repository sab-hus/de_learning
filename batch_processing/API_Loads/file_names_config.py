from google.cloud import bigquery


schema =  [
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("properties", "STRING"),
    bigquery.SchemaField("geometry", "STRING"),
    bigquery.SchemaField("id", "STRING")
]

api_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=20230813&endtime=20230814"
table_id = "dt-sabah-sandbox-dev.load_api_data.Features_Earthquakes"