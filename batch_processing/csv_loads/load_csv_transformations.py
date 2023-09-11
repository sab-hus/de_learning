import pandas as pd
from file_names import transform_data_file_1, transform_data_file_2
# generate below output file:
# legal_entity, counterparty(1,2), tier(2), max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)

df1 = pd.read_csv(transform_data_file_1)
df2 = pd.read_csv(transform_data_file_2)

final_df = df1.merge(df2, on="counter_party")
# should i include how=left param?
# print(final_df)

# max_rating_by_counterparty = final_df.groupby("counter_party")["rating"].max().reset_index()
# sum_value_arap = final_df[final_df['status'] == 'ARAP'].groupby("counter_party")["value"].sum().reset_index()
# sum_value_accr = final_df[final_df['status'] == 'ACCR'].groupby("counter_party")["value"].sum().reset_index()

# output_df = pd.DataFrame({
#     "legal_entity": final_df["legal_entity"],
#     "counterparty": final_df["counter_party"],
#     "tier": df2["tier"],
#     "max_rating_by_counterparty)": max_rating_by_counterparty["rating"],
#     "sum_value_where_status=ARAP)": sum_value_arap["value"],
#     "sum_value_where_status=ACCR)": sum_value_accr["value"]
# })

final_df = final_df.groupby(["legal_entity", "counter_party", "tier"]).agg(
    max_rating_by_counterparty=("rating", "max"),
    sum_value_arap=("value", lambda x: x[final_df["status"] == "ARAP"].sum()),
    sum_value_accr=("value", lambda x: x[final_df["status"] == "ACCR"].sum())
).reset_index()

final_df.columns = ["legal_entity", "counterparty", "tier", "max_rating_by_counterparty", "sum_value_status=ARAP", "sum_value_status=ACCR"]

final_df.to_csv("transformed_file.csv")



# # def find_max_magnitudes_per_planet(transform_file):
# #     try:
#         df = pd.read_csv(transform_file)
#         # Group the data by "Name" and find the maximum stellar magnitude for each planet
#         max_magnitudes_per_planet = df.groupby("Name")["STELLAR MAGNITUDE"].max()
#         df["MAX STELLAR MAGNITUDE"] = df["Name"].map(lambda name: f"Planet: '{name}', max magnitude: '{max_magnitudes_per_planet[name]}'")
#         df.to_csv(max_magnitude_transformed_file, index=False)
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")
#         return None
# find_max_magnitudes_per_planet(transform_file)



# 1.pandas functions 
# 2.pyspark use this to do same job
# clean code

# join the tables together
# find maximum value of ratings 
# sum of value for each different status


# concept in sql called grouping sets - combing a lot of aggregated views together
# look up and read grouping sets in SQL - use the same thing but in pandas
# generate table which contains the desired aggregation

#  if not making enough progross - find out max value for rating, sum value for 2 status, for different legal entity and counterparties