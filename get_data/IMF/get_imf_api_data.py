# %%
import requests
import pandas as pd
import os
import sys
import datetime
from pathlib import Path

# Adiciona o diretório pai ao sys.path
parent_dir = str(Path(__file__).resolve().parent.parent.parent)
sys.path.insert(1, parent_dir)
# Get the current file's name

import generic_auxiliary_functions as gaf
from delivery_methods.delivery_app import DeliveryApp
gaf = gaf.AuxiliarFunctions()

class GetIMF:
    def __init__(self):

        self.relative_path_to_output = os.path.join(
            Path(__file__).resolve().parent.parent.parent, "data_lake", "raw_data","macroeconomic_data"
        )

        # Get the current year
        current_year = datetime.datetime.now().year

        endpoint_indicators = "https://www.imf.org/external/datamapper/api/v1/indicators"
        endpoint_countries = "https://www.imf.org/external/datamapper/api/v1/countries"
        endpoint_regions = "https://www.imf.org/external/datamapper/api/v1/regions"
        endpoint_groups = "https://www.imf.org/external/datamapper/api/v1/groups"
        endpoint_gdps = "https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH"

        # Get the data
        indicators = self.get_data(endpoint_indicators)
        countries = self.get_data(endpoint_countries)
        regions = self.get_data(endpoint_regions)
        groups = self.get_data(endpoint_groups)
        gdps = self.get_data(endpoint_gdps)

        groups['groups']

        df_before_names = pd.DataFrame(gdps['values']['NGDP_RPCH']).reset_index().rename(columns={'index': 'year'}).set_index('year')
        df_country = pd.DataFrame.from_dict(countries['countries'], orient='index').reset_index().rename(columns={'index': 'code', 'label': 'country'})

        gdp_countries = df_before_names.copy()
        gdp_countries.columns = gdp_countries.columns.map(lambda x: df_country.set_index('code')['country'].get(x, x))

        # Define the complete path to the file including the file name
        file_name_imf_gdp = "imf_gdp.csv"
        file_path = os.path.join(self.relative_path_to_output, file_name_imf_gdp)

        # Save the DataFrame to a .pkl file
        gdp_countries.to_csv(file_path)

    def get_data(self, url):
        # get the endpoint
        try:
            response = requests.get(url)
        except requests.exceptions.SSLError:
            delivery = DeliveryApp(False)
            delivery.do_delivery_activity(
                type_method="send_text", text="Falta certificado."
            )
            # response = requests.get(url, verify=False)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None

if __name__ == "__main__":
    GetIMF()

# %%
