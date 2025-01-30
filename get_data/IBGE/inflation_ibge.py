# Quando eu pegar a inflacao, eu necessariamente SEMPRE vou querer pegar a abertura de inflacao?
# Não necessariamente!

# %%
import requests
import pandas as pd
import os

# url_history = "https://servicodados.ibge.gov.br/api/v3/agregados/2938/periodos/200607|200608|200609|200610|200611|200612|200701|200702|200703|200704|200705|200706|200707|200708|200709|200710|200711|200712|200801|200802|200803|200804|200805|200806|200807|200808|200809|200810|200811|200812|200901|200902|200903|200904|200905|200906|200907|200908|200909|200910|200911|200912|201001|201002|201003|201004|201005|201006|201007|201008|201009|201010|201011|201012|201101|201102|201103|201104|201105|201106|201107|201108|201109|201110|201111|201112/variaveis/63?localidades=N1[all]&classificacao=315[all]"
# url_open_itens= "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/202001|202002|202003|202004|202005|202006|202007|202008|202009|202010|202011|202012|202101|202102|202103|202104|202105|202106|202107|202108|202109|202110|202111|202112|202201|202202|202203|202204|202205|202206|202207|202208|202209|202210|202211|202212|202301|202302|202303|202304|202305|202306|202307/variaveis/63?localidades=N1[all]&classificacao=315[all]"
# get table number 7060
# This code will generate sequence
import datetime

class GetIPCA:
    def __init__(self):

        # Get the current year
        current_year = datetime.datetime.now().year

        # Generate the period sequence from 2020 to the current year plus one
        period_sequence = self.generate_period_sequence(2020, current_year + 1)

        # Construct the URLs
        base_url_ipca = "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/"
        url_ipca = f"{base_url_ipca}{period_sequence}/variaveis/63?localidades=N1[all]&classificacao=315[7169]"

        base_url_ipca15 = "https://servicodados.ibge.gov.br/api/v3/agregados/7062/periodos/"
        url_ipca15 = f"{base_url_ipca15}{period_sequence}/variaveis/355?localidades=N1[all]&classificacao=315[7169]"

        # Set the current working directory to the script's directory
        script_directory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_directory)

        # Define the relative path to the output folder from the script's directory
        relative_path_to_output = "../../data_lake/raw_data"

        # Create the output directory if it does not exist
        os.makedirs(relative_path_to_output, exist_ok=True)

        # Define the complete path to the file including the file name
        file_name_ipca = "inflation_ipca_data.pkl"
        file_name_ipca15 = "inflation_ipca15_data.pkl"
        file_path_ipca = os.path.join(relative_path_to_output, file_name_ipca)
        file_path_ipca15 = os.path.join(relative_path_to_output, file_name_ipca15)

        # Get the data
        inflation_ipca_data = self.get_data(url_ipca)
        inflation_ipca15_data = self.get_data(url_ipca15)

        if inflation_ipca15_data:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        if inflation_ipca_data:
            # print(inflation_ipca_data)
            # print(inflation_ipca15_data)
            print("Everything is ok")
        else:
            print("No data retrieved.")

        # Extract data
        ipca_series = inflation_ipca_data[0]["resultados"][0]["series"][0]["serie"]
        ipca15_series = inflation_ipca15_data[0]["resultados"][0]["series"][0]["serie"]

        # Convert dictionaries to dataframes
        df_ipca = pd.DataFrame(list(ipca_series.items()), columns=["Date", "IPCA"])
        df_ipca15 = pd.DataFrame(list(ipca15_series.items()), columns=["Date", "IPCA15"])

        # Save the DataFrame to a .pkl file
        df_ipca.to_pickle(file_path_ipca)
        df_ipca15.to_pickle(file_path_ipca15)

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
