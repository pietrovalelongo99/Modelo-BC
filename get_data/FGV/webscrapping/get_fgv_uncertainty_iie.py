# %

# This link worked:
# https://portalibre.fgv.br/system/files/divulgacao/noticias/mat-complementar/2025-01/iiebr_dez24.xlsx 

# Methodology is available in 
# https://portalibre.fgv.br/indicador-de-incerteza-da-economia

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import os
import sys
import time
from pathlib import Path

# Adiciona o diretório pai ao sys.path
parent_dir = str(Path(__file__).resolve().parent.parent.parent.parent)
sys.path.insert(1, parent_dir)

def download_xlsx_file(is_production = True):
    # is_production = True

    # Usage
    import generic_auxiliary_functions as gaf
    gaf = gaf.AuxiliarFunctions()

    # URL of the Excel file
    if is_production:
        this_month = datetime.now().strftime('%Y-%m')
        last_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%b%y').lower()
    else:
        this_month = (datetime.now().replace(day=1) + timedelta(days=32)).strftime('%Y-%m')
        last_month = (datetime.now().replace(day=1)).strftime('%b%y').lower()

    last_month = gaf._convert_month_to_pt(last_month, long=False)

    # Set the current working directory to the script's directory
    script_directory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_directory)

    # Set the current working directory to the script's directory
    script_directory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_directory)
    relative_path_to_output = os.path.join(
        script_directory, "..", "..", "..", "..", "data_lake", "raw_data","FGV","iiebr"
    )

    if os.path.exists(os.path.join(relative_path_to_output, f"iiebr_{last_month}.xlsx")):
        return

    # Get the directory of the currently running script
    current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    # Navigate up one folder (to the 'app' directory) and then to the chromedriver path
    chromedriver_path = os.path.join(current_dir, "..", "path", "to", "chromedriver.exe")

    # Start a new instance of Chrome with the existing user profile
    options = webdriver.ChromeOptions()
    # options.add_argument("user-data-dir=C:\\Users\\chris\\AppData\\Local\\Google\\Chrome\\Selenium")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": os.path.abspath(relative_path_to_output),  # Set default directory for downloads
            "download.prompt_for_download": False,  # Disable download prompt
            "download.directory_upgrade": True,
        },
    )

    # Path to chromedriver (update to your actual path)
    os.environ["PATH"] += os.pathsep + os.path.dirname(chromedriver_path)
    # Initialize the WebDriver
    driver = webdriver.Chrome(options=options)

    try:

        # This code worked perfetly
        # https://portalibre.fgv.br/system/files/divulgacao/noticias/mat-complementar/2025-01/iiebr_dez24.xlsx 
        url = f"https://portalibre.fgv.br/system/files/divulgacao/noticias/mat-complementar/{this_month}/iiebr_{last_month}.xlsx"
        driver.get(url)

        # Wait for the file to download
        time.sleep(10)  # Adjust this time if the download takes longer

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        # Close the browser
        driver.quit()

if __name__ == "__main__":
    download_xlsx_file()
