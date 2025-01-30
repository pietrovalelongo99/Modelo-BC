# %%
import requests
import pandas as pd
import plotly.express as px
import os



def get_bcb_data():
    # Set the current working directory to the script's directory
    script_directory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_directory)

    # Define the relative path to the output folder from the script's directory
    relative_path_to_output = "../../data_lake/raw_data"
    # Create the directory if it does not exist
    if not os.path.exists(relative_path_to_output):
        os.makedirs(relative_path_to_output)
    # Define the complete path to the file including the file name
    pickle_filename = "cdi.pkl"
    file_path = os.path.join(relative_path_to_output, pickle_filename)

    # Get the data from BCB
    df_cdi = get_cdi_rates()
    try:
        # Save DataFrame as a pickle file at the specified file_path
        df_cdi.to_pickle(file_path)
        print(f"File saved successfully at {file_path}")
    except Exception as e:
        print(f"Error occurred: {e}")

    pass

@staticmethod
def query_bcb_api(series_code):
    """
    Generic function to query the Brazilian Central Bank API.

    Args:
    series_code (int): The series code for the data to be retrieved.

    Returns:
    JSON: The response data in JSON format.
    """
    api_url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def get_cdi_rates():
    """
    Function to retrieve cdi rates.

    Returns:
    DataFrame: The cdi rates data as a pandas DataFrame.
    """
    cdi_series_code = 11  # Example series code, replace with the correct one
    cdi_data = query_bcb_api(cdi_series_code)
    if cdi_data:
        df = pd.DataFrame(cdi_data)
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
        df["valor"] = df["valor"].astype(float)  # Convert 'valor' to float
        return df.sort_values(by="data")
    else:
        return None

@staticmethod
def plot_data_with_plotly(df, x_column, y_column, plot_title, x_label, y_label):
    """
    Generic function to plot data using Plotly.

    Args:
    df (DataFrame): The DataFrame containing the data to plot.
    x_column (str): The name of the column to use for the x-axis.
    y_column (str): The name of the column containing numerical values to use for the y-axis.
    plot_title (str): The title of the plot.
    x_label (str): The label for the x-axis.
    y_label (str): The label for the y-axis.
    """
    fig = px.line(
        df,
        x=x_column,
        y=y_column,
        title=plot_title,
        labels={x_column: x_label, y_column: y_label},
    )
    fig.update_layout(xaxis_title=x_label, yaxis_title=y_label)
    fig.show()

    def update_cdi_table():
        # SAVE IN THE TABLE OF CDI!
        # "data_lake\data_extracted\data_base\tbl_90200_indice_cdi_diario.csv"
        cdi_df = get_cdi_rates()



# Testing the class
# %%
if __name__ == "__main__":
    cdi_df = get_cdi_rates()

    
    if cdi_df is not None:
        plot_data_with_plotly(
            cdi_df, "data", "valor", "CDI Overnight", "Date", "Interest Rate"
        )
