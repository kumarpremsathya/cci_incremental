from datetime import datetime
import mysql.connector
from selenium import webdriver

source_status = "Active"
source_name = "cci_section43a_44"


log_list = [None] * 8
no_data_avaliable = 0
no_data_scraped = 0
deleted_sources = ""
deleted_source_count = 0
updated_count = 0

url = 'https://www.cci.gov.in/combination/orders-section43a_44'

# download_folder= r"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\cci_43"

# bandit_path =r"C:\Users\Premkumar.8265\AppData\Roaming\Python\Python311\Scripts"

chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument(f"--disable-notifications")  
# chrome_options.add_experimental_option("prefs", {
#     "download.default_directory": download_folder,
#     "download.prompt_for_download": False,
#     "download.directory_upgrade": True
# })
browser = webdriver.Chrome(options=chrome_options)

current_date = datetime.now().strftime("%Y-%m-%d")



host = "localhost"
user = "root"
password = "root"
database = "cci"
# auth_plugin = "mysql_native_password"





# host = "4.213.77.165"
# user = "root1"
# password = "Mysql1234$"
# database = "cci"
# auth_plugin = "mysql_native_password"
def db_connection():
    connection = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database,
        # auth_plugin = auth_plugin

    )
    return connection

