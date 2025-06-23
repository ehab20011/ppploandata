import csv
from pathlib import Path

# Set your input CSV file path here
INPUT_CSV_PATH = Path("ppp_csvs/ppp.csv")
OUTPUT_CSV_PATH = INPUT_CSV_PATH.parent / "ppp_subset.csv"

def extract_first_10k_rows(input_path: Path, output_path: Path):
    with input_path.open("r", encoding="cp1252", newline='') as infile, \
         output_path.open("w", encoding="cp1252", newline='') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for i, row in enumerate(reader):
            writer.writerow(row)
            if i == 9999:  # 0-based index, 1 header + 9999 rows = 10,000 total
                break

    print(f"Extracted 10,000 rows to: {output_path}")

if __name__ == "__main__":
    extract_first_10k_rows(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
