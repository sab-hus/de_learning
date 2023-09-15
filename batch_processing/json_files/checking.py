import json

# Read the JSON file
with open('birthdays.json') as f:
    data = json.load(f)

# Check for trailing data
if data[-1]:
    raise ValueError('Trailing data found')