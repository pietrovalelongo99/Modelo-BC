import os
import requests
from datetime import datetime
import pandas as pd

def get_nino_table():
    current_dir = os.path.dirname(__file__)
    folder_path = os.path.join(current_dir, '..', '..', '..', 'data_lake', 'raw_data', 'nino_index')
    os.makedirs(folder_path, exist_ok=True)  # Ensure the directory exists

    # URL for El Niño index data
    url = "https://origin.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt"
    response = requests.get(url)

    if response.status_code == 200:
        raw_data = response.text
        lines = raw_data.strip().split("\n")

        # First row contains the headers
        headers = lines[0].split()

        # Parse the data
        data = []
        for line in lines[1:]:
            parts = line.split()
            if len(parts) == 5:
                data.append([int(parts[0]), int(parts[1]), float(parts[2]), float(parts[3]), float(parts[4])])

        # Convert to DataFrame
        df = pd.DataFrame(data, columns=headers)

        # Save as CSV
        date_str = datetime.now().strftime("%Y_%m_%d")
        file_path = os.path.join(folder_path, f'nino_index.csv')
        df.to_csv(file_path, index=False)

# %%
if __name__ == "__main__":
    get_nino_table()
