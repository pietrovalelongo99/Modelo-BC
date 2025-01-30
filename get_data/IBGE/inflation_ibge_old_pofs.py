# Aqui pega as inflações antigas:
# Ou seja, basta rodar apenas uma unica vez que será coletado tudo.

# Abertura + total

# %%
import requests
import pandas as pd
import os

# url_history = "https://servicodados.ibge.gov.br/api/v3/agregados/2938/periodos/200607|200608|200609|200610|200611|200612|200701|200702|200703|200704|200705|200706|200707|200708|200709|200710|200711|200712|200801|200802|200803|200804|200805|200806|200807|200808|200809|200810|200811|200812|200901|200902|200903|200904|200905|200906|200907|200908|200909|200910|200911|200912|201001|201002|201003|201004|201005|201006|201007|201008|201009|201010|201011|201012|201101|201102|201103|201104|201105|201106|201107|201108|201109|201110|201111|201112/variaveis/63?localidades=N1[all]&classificacao=315[all]"
# url_open_itens= "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/202001|202002|202003|202004|202005|202006|202007|202008|202009|202010|202011|202012|202101|202102|202103|202104|202105|202106|202107|202108|202109|202110|202111|202112|202201|202202|202203|202204|202205|202206|202207|202208|202209|202210|202211|202212|202301|202302|202303|202304|202305|202306|202307/variaveis/63?localidades=N1[all]&classificacao=315[all]"
# get table number 7060
# This code will generate sequence
import datetime

from pathlib import Path
import sys
# Adiciona o diretório pai ao sys.path
parent_dir = str(Path(__file__).resolve().parent.parent.parent)
sys.path.insert(1, parent_dir)
# Get the current file's name
import generic_auxiliary_functions as gaf
gaf = gaf.AuxiliarFunctions()

class GetIPCA:
    def __init__(self):

        # Set the current working directory to the script's directory
        script_directory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_directory)

        # Define the relative path to the output folder from the script's directory
        relative_path_to_output = "../../../data_lake/raw_data/macroeconomic_data/old_inflation"
        # Create the output directory if it does not exist
        os.makedirs(relative_path_to_output, exist_ok=True)


        # Generate the period sequence from 2020 to the current year plus one
        period_sequence_1999_2006 = self.generate_period_sequence(1999, 2006)
        # Construct the URLs - ago/1999 a jun/2006
        base_url_ipca_1999_2006 = "https://servicodados.ibge.gov.br/api/v3/agregados/655/periodos/"
        url_ipca_1999_2006 = f"{base_url_ipca_1999_2006}{period_sequence_1999_2006}/variaveis/63?localidades=N1[all]&classificacao=315[all]"
        # url_ipca_1999_2006_peso = f"{base_url_ipca_1999_2006}{period_sequence_1999_2006}/variaveis/63?localidades=N1[all]&classificacao=315[all]"

        period_sequence_2006_2011 = self.generate_period_sequence(2006, 2011)
        # Construct the URLs - jul/2006 a dez/2011
        base_url_ipca_2006_2011 = "https://servicodados.ibge.gov.br/api/v3/agregados/2938/periodos/"
        url_ipca_2006_2011 = f"{base_url_ipca_2006_2011}{period_sequence_2006_2011}/variaveis/63?localidades=N1[all]&classificacao=315[all]"

        period_sequence_2012_2019 = self.generate_period_sequence(2012, 2019)
        # Construct the URLs - jan/2012 a dez/2019
        base_url_ipca_2012_2019 = "https://servicodados.ibge.gov.br/api/v3/agregados/1419/periodos/"
        url_ipca_2012_2019 = f"{base_url_ipca_2012_2019}{period_sequence_2012_2019}/variaveis/63?localidades=N1[all]&classificacao=315[all]"

        # Get the data
        inflation_ipca_data_1999_2006 = self.get_data(url_ipca_1999_2006)
        inflation_ipca_data_2006_2011 = self.get_data(url_ipca_2006_2011)
        inflation_ipca_data_2012_2019 = self.get_data(url_ipca_2012_2019)

        if inflation_ipca_data_1999_2006:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        if inflation_ipca_data_2006_2011:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        if inflation_ipca_data_2012_2019:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        table_variation_1999_2006 = gaf.create_ipca_table(inflation_ipca_data_1999_2006)
        table_variation_2006_2011 = gaf.create_ipca_table(inflation_ipca_data_2006_2011)
        table_variation_2012_2019 = gaf.create_ipca_table(inflation_ipca_data_2012_2019)

        # Define the complete path to the file including the file name
        file_name_ipca_1999_2006 = "inflation_ipca_data_1999_2006.pkl"
        file_name_ipca_2006_2011 = "inflation_ipca_data_2006_2011.pkl"
        file_name_ipca_2012_2019 = "inflation_ipca_data_2012_2019.pkl"

        file_path_ipca_1999_2006 = os.path.join(relative_path_to_output, file_name_ipca_1999_2006)
        file_path_ipca_2006_2011 = os.path.join(relative_path_to_output, file_name_ipca_2006_2011)
        file_path_ipca_2012_2019 = os.path.join(relative_path_to_output, file_name_ipca_2012_2019)

        # Save the DataFrame to a .pkl file
        table_variation_1999_2006.to_pickle(file_path_ipca_1999_2006)
        table_variation_2006_2011.to_pickle(file_path_ipca_2006_2011)
        table_variation_2012_2019.to_pickle(file_path_ipca_2012_2019)

    def get_data(self, url):
        # get the endpoint
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None

    def generate_period_sequence(self, start_year, end_year):
        """Generate a sequence of periods from start_year to end_year inclusive."""
        periods = []
        # in range, end_year + 1 is exclusive.
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                periods.append(f"{year}{month:02d}")
        return "|".join(periods)

if __name__ == "__main__":
    GetIPCA()
