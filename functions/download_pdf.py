import os
import requests
import pandas as pd
from config import cci_config
from functions import insert_excel_sheet_data_to_mysql

    
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import time
import shutil
import Store_Data_Mysql
import sys
from datetime import datetime
import historical

# Function to configure and initialize the Chrome driver
def initialize_browser(download_path):
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(options=chrome_options)
    browser.maximize_window()
    return browser

# Function to read the "Order" column from the Excel file

def read_order_urls_from_excel(increment_data_excel_path):
    df = pd.read_excel(increment_data_excel_path)
    
    return df

def download_pdfs_web(df, download_dir, browser):
    df['orderpdf_file_name'] = ''
    df['orderpdf_file_path'] = ''
    failed_downloads = []

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    print("download_dir=======",download_dir)
   
    for index, row in df.iterrows():
        try:
            pdf_url = row['order_link']
            retries = 2
            print(f"pdf_url=== {pdf_url}")
            for attempt in range(retries):
                try:
                    browser.get(pdf_url)
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.ID, 'iframesrc'))
                    )
                    iframe = browser.find_element(By.ID, 'iframesrc')
                    pdf_name = iframe.get_attribute('src').split('/')[-1]
                    df.at[index, 'orderpdf_file_name'] = pdf_name

                    # Find the correct download link
                    download_links = browser.find_elements(By.XPATH, '//a[contains(@onclick, "DownloadFile")]')
                    correct_link = None
                    for link in download_links:
                        if pdf_name in link.get_attribute('onclick'):
                            correct_link = link
                            break

                    if not correct_link:
                        raise Exception(f"No correct download link found for {pdf_name}")

                    time.sleep(5)
                    correct_link.click()
                    time.sleep(10)
                    download_path = os.path.join(download_dir, pdf_name)
                    download_timeout = 60  # increased timeout for reliability
                    start_time = time.time()
                    
                    
                    # try:
                    #     download_button = browser.find_element(By.XPATH, "//table[@class='table']/tbody/tr[3]/td[2]/div/a[2]")
                    #     time.sleep(5)
                    #     download_button.click()
                    #     time.sleep(10)
                    #     download_path = os.path.join(download_dir, pdf_name)
                    #     download_timeout = 30
                    #     start_time = time.time()
                    # except Exception as e:
                    #     print("error occured====", e)
                    #     download_button = browser.find_element(By.XPATH, "//table[@class='table']/tbody/tr[4]/td[2]/div/a[2]")
                    #     time.sleep(5)
                    #     download_button.click()
                    #     time.sleep(10)
                    #     download_path = os.path.join(download_dir, pdf_name)
                    #     download_timeout = 30
                    #     start_time = time.time()

                    while not os.path.exists(download_path):
                        time.sleep(1)
                        if time.time() - start_time > download_timeout:
                            raise Exception(f"Download timed out for {pdf_name}")

                    decision_date = pd.to_datetime(row['decision_date'], format='%d/%m/%Y')
                    year_folder = os.path.join(download_dir, str(decision_date.year))
                    month_folder = os.path.join(year_folder, decision_date.strftime('%b'))
                    if not os.path.exists(month_folder):
                        os.makedirs(month_folder)

                    destination_path = os.path.join(month_folder, pdf_name)
                    shutil.move(download_path, destination_path)
                    
                    # Calculate relative path from the cci_43 directory
                    # cci_43_dir = r"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\cci_43"
                    relative_pdf_path = os.path.relpath(destination_path, download_dir)
                    
                    relative_pdf_path = os.path.join(os.path.basename(download_dir), relative_pdf_path)
                    
                    print("relative_pdf_path", relative_pdf_path)
                    df.at[index, 'orderpdf_file_path'] = relative_pdf_path
                    
                    print(f"Downloaded: {pdf_name} and stored in folder {month_folder}")
                    df = df.drop_duplicates(subset=['order_link'], keep='last')
                    # df.to_excel(os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders_with_pdf_names.xlsx'), index=False)
                    # print("df", df.to_string())
                    
                    final_excel_sheet_name = f"final_sheet_{cci_config.current_date}.xlsx"
                   
                    final_excel_sheet_path = fr"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\data\final_excel_sheet\{final_excel_sheet_name}"
                    
                    df.to_excel(final_excel_sheet_path, index=False)
                    # print("excel in download pdf", df)
                                
                    
                    break
                except Exception as download_error:
                    print(f"Attempt {attempt + 1} failed for {pdf_url}: {download_error}")
                    if attempt == retries - 1:
                        failed_downloads.append(row)
        except Exception as e:
            print(f"Error for row {index}: {e}")
               
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"Error occurred at line {exc_tb.tb_lineno}:")
            print(f"Exception Type: {exc_type}")
            print(f"Exception Object: {exc_obj}")
            print(f"Traceback: {exc_tb}")

    return df, failed_downloads



def save_failed_downloads(failed_downloads, filepath):
    if failed_downloads:
        failed_df = pd.DataFrame(failed_downloads)
        failed_df.to_excel(filepath, index=False)
    else:
        print("No failed downloads to save.")
        


def retry_failed_downloads(download_dir, browser):
    failed_downloads_file = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'failed_downloads.xlsx')

    if not os.path.exists(failed_downloads_file):
        print("No failed downloads to retry.")
        return

    df = pd.read_excel(failed_downloads_file)
    final_excel_path = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders_with_pdf_names.xlsx')
    final_df = pd.read_excel(final_excel_path)

    for index, row in df.iterrows():
        try:
            pdf_url = row['PDF URL']
            retries = 2
            for attempt in range(retries):
                try:
                    browser.get(pdf_url)
                    WebDriverWait(browser, 20).until(
                        EC.presence_of_element_located((By.ID, 'iframesrc'))
                    )
                    iframe = browser.find_element(By.ID, 'iframesrc')
                    pdf_name = iframe.get_attribute('src').split('/')[-1]

                    # Find the correct download link
                    download_links = browser.find_elements(By.XPATH, '//a[contains(@onclick, "DownloadFile")]')
                    correct_link = None
                    for link in download_links:
                        if pdf_name in link.get_attribute('onclick'):
                            correct_link = link
                            break

                    if not correct_link:
                        raise Exception(f"No correct download link found for {pdf_name}")

                    time.sleep(5)
                    correct_link.click()
                    time.sleep(10)
                    download_path = os.path.join(download_dir, pdf_name)
                    download_timeout = 60  # increased timeout for reliability
                    start_time = time.time()

                    while not os.path.exists(download_path):
                        time.sleep(1)
                        if time.time() - start_time > download_timeout:
                            raise Exception(f"Download timed out for {pdf_name}")

                    decision_date = pd.to_datetime(row['Decision Date'], format='%d/%m/%Y')
                    year_folder = os.path.join(download_dir, str(decision_date.year))
                    month_folder = os.path.join(year_folder, decision_date.strftime('%b'))
                    if not os.path.exists(month_folder):
                        os.makedirs(month_folder)

                    destination_path = os.path.join(month_folder, pdf_name)
                    shutil.move(download_path, destination_path)
                    relative_pdf_path = os.path.relpath(destination_path)
    
                    row['PDF Name'] = pdf_name 
                    row['PDF Path'] = relative_pdf_path
                    
                    print(f"Downloaded: {pdf_name} and stored in folder {month_folder}")
                    final_df = pd.concat([final_df, pd.DataFrame([row])], ignore_index=True).drop_duplicates(subset=['PDF URL'], keep='last')
                    final_df.to_excel(final_excel_path, index=False)
                    

                  
                    break
                except Exception as download_error:
                    print(f"Attempt {attempt + 1} failed for {pdf_url}: {download_error}")
        except Exception as e:
            print(f"Error retrying {pdf_url}: {e}")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(f"Error occurred at line {exc_tb.tb_lineno}:")
            print(f"Exception Type: {exc_type}")
            print(f"Exception Object: {exc_obj}")
            print(f"Traceback: {exc_tb}")



def download_pdf(increment_data_excel_path):
    print("download pdf funtion is called")
    try:
        
        download_dir = r"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\cci_43"
       
        # base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cci_incremental')
        # print("base_dir====", base_dir)
        # download_dir = os.path.join(base_dir, "cci_43")
        browser = initialize_browser(download_dir)
        
        
        df = read_order_urls_from_excel(increment_data_excel_path)
        df,failed_downloads = download_pdfs_web(df, download_dir, browser)
        browser.quit()

        # if failed_downloads:
        #     save_failed_downloads(failed_downloads, os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'failed_downloads.xlsx'))

        # browser = initialize_browser(download_dir)
        # retry_failed_downloads(download_dir, browser)
        # browser.quit()
        
        
        final_excel_sheet_name = f"final_sheet_{cci_config.current_date}.xlsx"
        
        final_excel_sheet_path = fr"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\data\final_excel_sheet\{final_excel_sheet_name}"
        # df.to_excel(final_excel_sheet_path, index=False)
        print("final excel path in download\n", df.to_string)
       
        insert_excel_sheet_data_to_mysql.insert_excel_data_to_mysql(final_excel_sheet_path)
        
    except Exception as e:
        print("Error in download_pdf  :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
                


   