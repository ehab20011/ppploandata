import csv
from models import PPPLoanDataSchema
from pydantic import ValidationError

csv_path = "ppp.csv"
success_count = 0
error_count = 0

def clean_row(row):
    return {k: (v if v.strip() != "" else None) for k, v in row.items()}

with open(csv_path, newline='', encoding='cp1252') as csvfile:
    reader = csv.DictReader(csvfile)
    for i, row in enumerate(reader, 1):
        try:
            cleaned_row = clean_row(row)
            model = PPPLoanDataSchema(**cleaned_row)
            success_count += 1
        except ValidationError as e:
            print(f"Row {i} validation error:")
            print(e)
            error_count += 1

print(f"\nValidation complete. Success: {success_count}, Errors: {error_count}")