import pandas as pd
from file_names import transform_data_file_1, transformed_file_1, transformed_file_2

# find out max value for rating
def add_max_value_rating(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    max_rating_by_counterparty = df.groupby("counter_party")["rating"].max()
    df["max_rating_by_counterparty"] = max_rating_by_counterparty
    print(df)
    df.to_csv(output_csv, index=False)
    
input_csv = transform_data_file_1
output_csv = transformed_file_1
add_max_value_rating(input_csv, output_csv)

# sum value for 2 status for different legal entity and counterparties

def add_sum_value_each_status_legal_entity(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    aggregated_df_1 = df[df['status'] == 'ARAP'].groupby(['legal_entity', 'counter_party'])['value'].sum()
    aggregated_df_2 = df[df['status'] == 'ACCR'].groupby(['legal_entity', 'counter_party'])['value'].sum()
    aggregated_df_1 = aggregated_df_1.to_frame(name='sum_value_ARAP_status').reset_index()
    aggregated_df_2 = aggregated_df_2.to_frame(name='sum_value_ACCR_status').reset_index()
    aggregated_df = aggregated_df_1.merge(aggregated_df_2, on=['legal_entity', 'counter_party'], how='outer')
    aggregated_df.to_csv(output_csv, index=False)

    print(aggregated_df)

input_csv = transform_data_file_1
output_csv = transformed_file_2
add_sum_value_each_status_legal_entity(input_csv, output_csv)


