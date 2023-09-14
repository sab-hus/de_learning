from google.cloud import bigquery

def generate_json_file_table_pairs(*args):
    file_table_pairs = []
    for file, table in args:
        file_table_pairs.append((
            f"/Users/sabahhussain/learning_development/batch_processing/json_files/{file}",
            f"dt-sabah-sandbox-dev.load_json.{table}"
        ))
    return file_table_pairs

json_file_table_pairs = generate_json_file_table_pairs(
    ("people.json", "People"),
    ("colours.json", "Colours"),
    ("fruit.json", "Fruit"),
    ("birthdays.json", "Birthdays")
)

schema = [
    bigquery.SchemaField("name", "string"),
    bigquery.SchemaField("age", "integer")
    ]

file_for_schema_load = "/Users/sabahhussain/learning_development/batch_processing/json_files/people.json"
table_id_json_schema_load = "dt-sabah-sandbox-dev.load_json_with_schema.people_schema"