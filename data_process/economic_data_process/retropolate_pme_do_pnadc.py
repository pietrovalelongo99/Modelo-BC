# pip install -U kaleido
# nao bate com o do valor economico:
# https://s2-valor.glbimg.com/IPZM87wQAXa28F0rcgduRnYUJj4=/984x0/smart/filters:strip_icc()/i.s3.glbimg.com/v1/AUTH_63b422c2caee4269b8b34177e8876b93/internal_photos/bs/2020/5/P/nsfmevS5qWUbzCAFQ4tw/arte01bra-102-bruno-a10.jpg

# %%
import os
import pandas as pd
import plotly.graph_objects as go
import sys
from pandas.tseries.offsets import BDay
import numpy as np
from sklearn.linear_model import LinearRegression

# Add the directory containing 'generic_auxiliary_functions.py' to the system path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

import generic_auxiliary_functions as gaf
from get_data.IBGE.pme import GetPME
from get_data.IBGE.pnadc import GetPNADc

class RetropolatePMEToPNADc:
    def __init__(self):
        self.aufun = gaf.AuxiliarFunctions()

        # Set the current working directory to the script's directory
        script_directory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_directory)

        GetPNADc()
        GetPME()

        # Initialize paths and filenames.
        self.relative_path_to_raw_data = os.path.join(
            os.getcwd(), "..","..", "data_lake", "raw_data", "macroeconomic_data"
        )

        # Define the filenames
        filenames = [
            "pme_data_old.pkl",
            "pme_data.pkl",
            "pnadc_data.pkl"
        ]

        # Create a dictionary to store the dataframes
        dataframes = {}

        # Read each file and store it in the dictionary
        for filename in filenames:
            file_path = os.path.join(self.relative_path_to_raw_data, filename)
            dataframes[filename] = pd.read_pickle(file_path)

        pme_data_old = dataframes["pme_data_old.pkl"][0]['resultados'][0]['series'][0]['serie']
        pme_data = dataframes["pme_data.pkl"][0]['resultados'][0]['series'][0]['serie']
        pnadc_data = dataframes["pnadc_data.pkl"][0]['resultados'][0]['series'][0]['serie']

        pme_data_old = self.transform_json_dataframe(pme_data_old)
        pme_data = self.transform_json_dataframe(pme_data)
        pnadc_data = self.transform_json_dataframe(pnadc_data)

        merged_table = self.merge_and_transform(pme_data, pnadc_data, pme_data_old, take_log = True)

        self.plot_combined_series(merged_table)

        merged_table.to_csv(os.path.join(self.relative_path_to_raw_data, "pnadc_retropolated.csv"))

    def merge_and_transform(self, pme_data, pnadc_data, pme_data_old, take_log = False):
        """
        Realiza a regressão linear das log-diferenças de pnadc_data e pme_data
        e aplica a transformação aos dados de pme_data para criar uma série combinada.

        :param pme_data: DataFrame com colunas ['Data', 'Valor'] para pme_data
        :param pnadc_data: DataFrame com colunas ['Data', 'Valor'] para pnadc_data
        :return: Série combinada de todos os dados ajustados
        """

        # self.plot_common_data(pme_data_old, pme_data)

        # Combina os dados ajustados e originais
        # pme_data_merged = pd.concat([
        #     pme_data_old,
        #     pme_data
        # ]).sort_index()

        # Combina as séries para encontrar as datas em comum
        pme_data['pme_3mma'] = pme_data['Valor'].rolling(window=3).mean()
        pme_data_old['pme_old_3mma'] = pme_data['Valor'].rolling(window=3).mean()

        #  Merge pnadc with pme
        merged_data_for_regression = pd.merge(pme_data, pnadc_data, on='Data', suffixes=('_pme', '_pnadc')).dropna()

        if take_log == True:
            # Calcula log-diferenças para a regressão
            merged_data_for_regression['log_diff_pme_3mma'] = np.log(merged_data_for_regression['pme_3mma']).diff()
            merged_data_for_regression['log_diff_pnadc'] = np.log(merged_data_for_regression['Valor_pnadc']).diff()
        else:
            # Calcula log-diferenças para a regressão
            merged_data_for_regression['diff_pme_3mma'] = merged_data_for_regression['pme_3mma'].diff()
            merged_data_for_regression['diff_pnadc'] = merged_data_for_regression['Valor_pnadc'].diff()

        # Remove valores NaN resultantes do cálculo de diferenças
        merged_data_for_regression = merged_data_for_regression.dropna()

        if take_log == True:
            # self.plot_log_differences(merged_data_for_regression, 'Data', 'log_diff_pme_3mma', 'log_diff_pnadc')
            print("plot here")
        else:
            # self.plot_log_differences(merged_data_for_regression, 'Data', 'diff_pme_3mma', 'diff_pnadc')
            print("plot here")

        if take_log == True:
            # Realiza a regressão linear
            X = merged_data_for_regression[['log_diff_pme_3mma']].values
            y = merged_data_for_regression['log_diff_pnadc'].values
        else:
            # Realiza a regressão linear
            X = merged_data_for_regression[['diff_pme_3mma']].values
            y = merged_data_for_regression['diff_pnadc'].values

        model = LinearRegression().fit(X, y)
        print(model.intercept_, model.coef_)

        first_date = pnadc_data['Data'].iloc[0]

        # Aplica a transformação a todos os dados de pme_data e pme_data_old
        # (para garantir que pme_3mma esteja calculado em pme_data antes, caso não esteja)
        if take_log == True:
            pme_data['log_diff_pme_3mma'] = np.log(pme_data['pme_3mma']).diff()
            pme_data_old['log_diff_pme_old_3mma'] = np.log(pme_data_old['pme_old_3mma']).diff()

            # Usa a regressão para prever log_diff_pnadc
            pme_without_pnad = pme_data[pme_data["Data"]<first_date]

            pme_without_pnad['log_diff_pnadc_y'] = model.predict(
                pme_without_pnad[['log_diff_pme_3mma']].fillna(0).values
            )

            last_available_pnadc = pnadc_data['Valor'].iloc[0]
            current_ln_y = np.log(last_available_pnadc)
            inverted_diffs  = pme_without_pnad['log_diff_pnadc_y'][::-1]
            log_y_pred_list = []
            for diff_val in inverted_diffs:
                current_ln_y = current_ln_y - diff_val
                log_y_pred_list.append(current_ln_y)

            log_y_pred_list = log_y_pred_list[::-1]

            pme_without_pnad['pnadc_y'] = np.exp(log_y_pred_list)

        else:
            pme_data['diff_pme_3mma'] = pme_data['pme_3mma'].diff()
            pme_data_old['diff_pme_old_3mma'] = pme_data_old['pme_old_3mma'].diff()

            # Usa a regressão para prever log_diff_pnadc
            pme_without_pnad = pme_data[pme_data["Data"]<first_date]

            # Usa a regressão para prever log_diff_pnadc
            pme_without_pnad['diff_pnadc_y'] = model.predict(
                pme_without_pnad[['diff_pme_3mma']].fillna(0).values
            )

            # Reconstituir os valores ajustados a partir das log-diferenças
            cumsum_inverted = pme_without_pnad['diff_pnadc_y'][::-1].cumsum()
            added_last_value = -cumsum_inverted + pnadc_data['Valor'].iloc[0]
            pme_without_pnad['pnadc_y'] = added_last_value[::-1]

            # px.line(pme_data, y='pnadc_y').show()

        # Combina os dados ajustados e originais
        combined_series = pd.concat([
            pnadc_data.set_index('Data')['Valor'],
            pme_without_pnad.set_index('Data')['pnadc_y']
        ]).sort_index()

        # Make sure there is no same index in table.
        assert not combined_series.index.duplicated().any()

        return combined_series

    def plot_common_data(self, pme_data_old, pme_data):
        # Convert 'Data' columns to datetime for both dataframes
        pme_data_old['Data'] = pd.to_datetime(pme_data_old['Data'])
        pme_data['Data'] = pd.to_datetime(pme_data['Data'])

        # Find common dates
        common_dates = set(pme_data_old['Data']).intersection(set(pme_data['Data']))

        # Filter both dataframes to only include common dates
        filtered_old = pme_data_old[pme_data_old['Data'].isin(common_dates)]
        filtered_new = pme_data[pme_data['Data'].isin(common_dates)]

        # Create the plot
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=filtered_old['Data'],
            y=filtered_old['Valor'],
            mode='lines',
            name='Old PME'
        ))
        fig.add_trace(go.Scatter(
            x=filtered_new['Data'],
            y=filtered_new['Valor'],
            mode='lines',
            name='New PME'
        ))

        fig.update_layout(
            title="Common Data Comparison",
            xaxis_title="Date",
            yaxis_title="Valor",
            legend_title="Dataset",
            template="plotly_white"
        )

        fig.show()

    def transform_json_dataframe(self,df):
        """
        Transforma o dicionário JSON pme_data_old em um DataFrame pandas.
        
        :param pme_data_old: Dicionário com dados no formato {'YYYYMM': 'value'}
        :return: DataFrame com colunas 'AnoMes' e 'Valor'
        """
        # Convertendo o dicionário em um DataFrame
        df = pd.DataFrame(list(df.items()), columns=['AnoMes', 'Valor'])
        
        # Criando a coluna 'Data' com o primeiro dia do mês
        df['Data'] = pd.to_datetime(df['AnoMes'], format='%Y%m')
        
        # Convertendo o valor para float
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        
        # Mantendo apenas as colunas 'Data' e 'Valor'
        df = df[['Data', 'Valor']]
        
        return df

    def plot_log_differences(self, dataframe, date_column, pme_column, pnadc_column):
        """
        Plots log differences of PME and PNADC using Plotly.
        
        Parameters:
            dataframe (pd.DataFrame): The dataframe containing the data.
            date_column (str): The name of the column with date values.
            pme_column (str): The name of the column with log differences of PME.
            pnadc_column (str): The name of the column with log differences of PNADC.
        
        Returns:
            None: Displays the plot.
        """
        # Ensure the date column is in datetime format
        dataframe[date_column] = pd.to_datetime(dataframe[date_column])
        
        # Create plotly figure
        fig = go.Figure()
        
        # Add PME log differences to the plot
        fig.add_trace(go.Scatter(
            x=dataframe[date_column],
            y=dataframe[pme_column],
            mode='lines+markers',
            name=pme_column
        ))
        
        # Add PNADC log differences to the plot
        fig.add_trace(go.Scatter(
            x=dataframe[date_column],
            y=dataframe[pnadc_column],
            mode='lines+markers',
            name=pnadc_column
        ))
        
        # Update layout
        fig.update_layout(
            title='Log Differences of PME and PNADC',
            xaxis_title='Date',
            yaxis_title='Log Difference',
            template='plotly_white'
        )
        
        # Show the plot
        fig.show()

    def plot_combined_series(self, combined_series):
        """
        Plots the combined series using Plotly.

        :param combined_series: Pandas Series with DateTime index and values to plot
        """
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=combined_series.index,
            y=combined_series.values,
            mode='lines',
            name='Combined Series'
        ))

        fig.update_layout(
            title="Combined Series Over Time",
            xaxis_title="Date",
            yaxis_title="Value",
            template="plotly_white"
        )

        fig.show()

# %%
if __name__ == "__main__":
    # Create an instance of IPCAPlotter to execute the code.
    plotter = RetropolatePMEToPNADc()
