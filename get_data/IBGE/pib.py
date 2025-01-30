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

class GetPIB:
    def __init__(self):

        # Get the current year
        current_year = datetime.datetime.now().year

        # Construct the URLs
        first_year_to_get_data = 1996

        base_url_pib = "https://servicodados.ibge.gov.br/api/v3/agregados/1621/periodos/"
        final_part = "/variaveis/584?localidades=N1[all]&classificacao=11255[all]"

        url_ibge = f"{base_url_pib}{gaf.generate_ibge_period_sequence(first_year_to_get_data, current_year + 1, trimester = True)}{final_part}"
        # url_ibge = "https://servicodados.ibge.gov.br/api/v3/agregados/8163/periodos/201101|201102|201103|201104|201105|201106|201107|201108|201109|201110|201111|201112|201201|201202|201203|201204|201205|201206|201207|201208|201209|201210|201211|201212|201301|201302|201303|201304|201305|201306|201307|201308|201309|201310|201311|201312|201401|201402|201403|201404|201405|201406|201407|201408|201409|201410|201411|201412|201501|201502|201503|201504|201505|201506|201507|201508|201509|201510|201511|201512|201601|201602|201603|201604|201605|201606|201607|201608|201609|201610|201611|201612|201701|201702|201703|201704|201705|201706|201707|201708|201709|201710|201711|201712|201801|201802|201803|201804|201805|201806|201807|201808|201809|201810|201811|201812|201901|201902|201903|201904|201905|201906|201907|201908|201909|201910|201911|201912|202001|202002|202003|202004|202005|202006|202007|202008|202009|202010|202011|202012|202101|202102|202103|202104|202105|202106|202107|202108|202109|202110|202111|202112|202201|202202|202203|202204|202205|202206|202207|202208|202209|202210|202211|202212|202301|202302|202303|202304|202305|202306|202307|202308|202309|202310|202311|202312|202401|202402|202403/variaveis/7167|7168|11623|11624|11625|11626?localidades=N1[all]&classificacao=11046[all]|1274[all]"
        # response = requests.get(url_ibge_1)

        # Set the current working directory to the script's directory
        script_directory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_directory)

        # Define the relative path to the output folder from the script's directory
        relative_path_to_output = "../../data_lake/raw_data"

        # Create the output directory if it does not exist
        os.makedirs(relative_path_to_output, exist_ok=True)

        # Define the complete path to the file including the file name
        file_name_ibge = "pib_data.pkl"
        file_path_output = os.path.join(relative_path_to_output, file_name_ibge)

        # Get the data
        ibge_data = self.get_data(url_ibge)

        if ibge_data:
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

        # Save the DataFrame to a .pkl file
        pd.to_pickle(ibge_data, file_path_output)

    def get_data(self,url):
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
    GetPIB()
