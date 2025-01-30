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

class GetPME:
    def __init__(self):

        # Get the current year
        current_year = datetime.datetime.now().year

        # Construct the URLs - IT CAN GET OLDER!
        first_year_to_get_data = 1995

        # get 1995 to 2002
        base_url_pme_old = "https://servicodados.ibge.gov.br/api/v3/agregados/14/periodos/"
        final_part_old = (
            "/variaveis/9?localidades=N7[2901]"
        )

        base_url_pme = (
            "https://servicodados.ibge.gov.br/api/v3/agregados/2176/periodos/"
        )
        final_part = "/variaveis/1031?localidades=N110[all]&classificacao=2[6794]"

        url_ibge_old = f"{base_url_pme_old}{gaf.generate_ibge_period_sequence(first_year_to_get_data, current_year + 1)}{final_part_old}"
        url_ibge = f"{base_url_pme}{gaf.generate_ibge_period_sequence(first_year_to_get_data, current_year + 1)}{final_part}"

        # Set the current working directory to the script's directory
        script_directory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_directory)

        # Define the relative path to the output folder from the script's directory
        relative_path_to_output = "../../data_lake/raw_data/macroeconomic_data"

        # Create the output directory if it does not exist
        os.makedirs(relative_path_to_output, exist_ok=True)

        # Get the data
        ibge_data_old = self.get_data(url_ibge_old)
        ibge_data = self.get_data(url_ibge)

        # print to see PME variables.
        ibge_data_old[0]['resultados'][0]['series'][0]['serie']
        ibge_data[0]['resultados'][0]['series'][0]['serie']

        if ibge_data_old:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        # Define the complete path to the file including the file name
        file_name_ibge_old = "pme_data_old.pkl"
        file_path_output_old = os.path.join(relative_path_to_output, file_name_ibge_old)

        file_name_ibge = "pme_data.pkl"
        file_path_output = os.path.join(
            relative_path_to_output, file_name_ibge
        )

        # Save the DataFrame to a .pkl file
        pd.to_pickle(ibge_data_old, file_path_output_old)
        pd.to_pickle(ibge_data, file_path_output)

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
    GetPME()
