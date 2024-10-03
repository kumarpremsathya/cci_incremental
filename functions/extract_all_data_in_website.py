import os
import sys
import time
import requests
import traceback
import pandas as pd
from config import cci_config
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from functions import get_data_count_database, check_increment_data, send_mail, log


# taken_data = {
#     "final_orders" : [],
#     "link" : [],
# }


browser = cci_config.browser
all_data = []  # Define as global variable


# Function to scrape data from the table on the current page
def scrape_table(browser):
    data = []
    try:
        time.sleep(5)
        WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table#datatable_ajax tbody tr'))
        )
        rows = browser.find_elements(By.CSS_SELECTOR, 'table#datatable_ajax tbody tr')
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            cols_text = [col.text for col in cols]

            # Extract the anchor tag link
            anchor_tag = cols[-1].find_element(By.TAG_NAME, 'a')
            link = anchor_tag.get_attribute('href')
            cols_text[-1] = link
            # Skip the first column (No.) and only add the relevant data
            data.append(cols_text[1:])
    except Exception as e:
        print(f"Error in scrape_table: {e}")
    return data


# Function to handle pagination and scrape all pages
def extract_all_data_in_website():
    global all_data  # Use the global variable
    print("extract_all_data_in_website function is called")

    try:
        # Try to open the URL
        try:
            browser.get(cci_config.url)
            time.sleep(5)
            browser.maximize_window()
            time.sleep(2)
            next_button = browser.find_element(By.CSS_SELECTOR, '#datatable_ajax_next a')
            time.sleep(2)
        except (TimeoutException, WebDriverException, NoSuchElementException) as e:
            raise Exception("Website not opened correctly") from e
        

        # Scrape the first page
        first_page_data = scrape_table(browser)
        all_data.extend(first_page_data)
        # Print the data from the first page to the terminal
        print("First page data:")
        for row in first_page_data:
            print("row=======", row)
        while True:
            try:
                time.sleep(5)
                next_button = browser.find_element(By.CSS_SELECTOR, '#datatable_ajax_next a')
                if 'disabled' in next_button.get_attribute('class'):
                    break
                else:
                    next_button.click()
                    # Wait for the table to refresh
                    WebDriverWait(browser, 20).until(
                        EC.staleness_of(browser.find_element(By.CSS_SELECTOR, 'table#datatable_ajax tbody tr'))
                    )
                    # Scrape the data from the new page
                    all_data.extend(scrape_table(browser))
                    
            except (TimeoutException, NoSuchElementException) as e:
                print(f"Error in pagination: {e}")
                break
            except Exception as e:
                print(f"Unexpected error in pagination: {e}")
                break
                # return all_data
                # Convert data to DataFrame


        columns = [
            'combination_reg_no',
            'description',
            'under_section',
            'decision_date',
            'order_link']
        df = pd.DataFrame(all_data, columns=columns)
        first_excel_sheet_name = f"first_excel_sheet_{cci_config.current_date}.xlsx"
        # first_exceL_sheet_path = rf"C:\Users\mohan.7482\Desktop\CCI\incremental_cci_anti_profiteering\data\first_excel_sheet\{first_excel_sheet_name}"
        first_exceL_sheet_path = rf"C:\Users\Premkumar.8265\Desktop\cci_project_personal\cci_incremental\data\first_excel_sheet\{first_excel_sheet_name}"
     
        df.to_excel(first_exceL_sheet_path, index = False)
        print("df========\n\n", df.to_string( ))
        # check_increment_data.check_increment_data(first_exceL_sheet_path)
    except Exception as e:
        cci_config.log_list[1] = "Failure"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()

        if str(e) == "Website not opened correctly":
            cci_config.log_list[5] = "Website is not opened"
        else:
            cci_config.log_list[5] = "Error in data extraction part"
        
        print("error in data extraction part======", cci_config.log_list)
        log.insert_log_into_table(cci_config.log_list)
        cci_config.log_list = [None] * 8
        traceback.print_exc()
        send_mail.send_email("cci section 43 orders extract data in website error", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")



    

