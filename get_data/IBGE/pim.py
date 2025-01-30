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

class GetPIM:
    def __init__(self):
        # Get the current year
        current_year = datetime.datetime.now().year

        # Construct the URLs
        first_year_to_get_data = 2002

        base_url_pim = "https://servicodados.ibge.gov.br/api/v3/agregados/8888/periodos/"
        final_part_pim = "/variaveis/12607?localidades=N1[all]&classificacao=544[all]"

        url_ibge = f"{base_url_pim}{gaf.generate_ibge_period_sequence(first_year_to_get_data, current_year + 1)}{final_part_pim}"

        # Set the current working directory to the script's directory
        script_directory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_directory)

        # Define the relative path to the output folder from the script's directory
        relative_path_to_output = "../../data_lake/raw_data"

        # Create the output directory if it does not exist
        os.makedirs(relative_path_to_output, exist_ok=True)

        # Get the data
        ibge_data_pim = self.get_data(url_ibge)

        if ibge_data_pim:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        # See the data
        # ibge_series = ibge_data[1]['resultados'][0]['series'][0]['serie']
        # Convert dictionaries to dataframes
        # df_data = pd.DataFrame(list(ibge_series.items()), columns=['date', 'data'])
        # df_data['date'] = pd.to_datetime(df_data['date'], format='%Y%m')
        # df_data['data'] = df_data['data'].astype(float)
        # import plotly.express as px
        # fig = px.bar(df_data, x='date', y='data')
        # fig.show()

        # Define the complete path to the file including the file name
        file_name_ibge_pim = "pim_data.pkl"
        file_path_output_pim = os.path.join(relative_path_to_output, file_name_ibge_pim)

        # Save the DataFrame to a .pkl file
        pd.to_pickle(ibge_data_pim, file_path_output_pim)


    def get_data(self, url):
        # get the endpoint
        try:
            response = requests.get(url)
        except requests.exceptions.SSLError:
            delivery = DeliveryApp(False)
            delivery.do_delivery_activity(
                type_method="send_text", text="Falta certificado no IBGE."
            )
            # response = requests.get(url, verify=False)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None

if __name__ == "__main__":
    GetPIM()