from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
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


# Configure ChromeOptions
chrome_options = webdriver.ChromeOptions()

# Function to initialize and return the Chrome browser
def initialize_browser():
    browser = webdriver.Chrome(options=chrome_options)
    browser.maximize_window()  # Maximize the browser window
    return browser

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

            data.append(cols_text)
    except Exception as e:
        print(f"Error in scrape_table: {e}")
    return data

# Function to handle pagination and scrape all pages
def scrape_all_pages(browser):
    all_data = []
    
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
        except Exception as e:
            print(f"Error in scrape_all_pages: {e}")
            break
    return all_data

# Function to process the scraped data and save it to an Excel file
def process_scraped_data():
    # Initialize the browser and open the webpage
    browser = initialize_browser()
    url = 'https://www.cci.gov.in/combination/orders-section43a_44'
    browser.get(url)

    # Scrape all pages
    data = scrape_all_pages(browser)

    # Convert data to DataFrame
    columns = ['No.',
               'Combination Registration No.',
               'Description',
               'Under Section',
               'Decision Date',
               'Order']
    df = pd.DataFrame(data, columns=columns)

    # Save the data to an Excel file
    df.to_excel('cci_orders.xlsx', index=False)

    # Close the browser
    browser.quit()

    # Print the DataFrame
    print("df======", df)







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
# def read_order_urls_from_excel(excel_file_path, start_row, num_rows):
def read_order_urls_from_excel(excel_file_path):
    df = pd.read_excel(excel_file_path)
    df.rename(columns={'Order': 'PDF URL'}, inplace=True)
    # df = df.iloc[start_row:start_row + num_rows]
    return df

def download_pdfs(df, download_dir, browser):
    df['PDF Name'] = ''
    df['PDF Path'] = ''
    failed_downloads = []

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    for index, row in df.iterrows():
        try:
            pdf_url = row['PDF URL']
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
                    df.at[index, 'PDF Name'] = pdf_name

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

                    decision_date = pd.to_datetime(row['Decision Date'], format='%d/%m/%Y')
                    year_folder = os.path.join(download_dir, str(decision_date.year))
                    month_folder = os.path.join(year_folder, decision_date.strftime('%b'))
                    if not os.path.exists(month_folder):
                        os.makedirs(month_folder)

                    destination_path = os.path.join(month_folder, pdf_name)
                    shutil.move(download_path, destination_path)
                    relative_pdf_path = os.path.relpath(destination_path)
                    df.at[index, 'PDF Path'] = relative_pdf_path
                    
                    print(f"Downloaded: {pdf_name} and stored in folder {month_folder}")
                    df = df.drop_duplicates(subset=['PDF URL'], keep='last')
                    df.to_excel(os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders_with_pdf_names.xlsx'), index=False)
                    # print("df", df.to_string())
                    
                    
            
            
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

    return failed_downloads



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



def main():
    try:
        # download_dir = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", "cci_43")
        base_dir = os.path.dirname(os.path.abspath(__file__))  # Base directory for the project
        download_dir = os.path.join(base_dir, "cci_43")
        browser = initialize_browser(download_dir)
        
        process_scraped_data()
        
        
        excel_file_path = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders.xlsx')
        # start_row = 0
        # num_rows = 2
        
        
        # df = read_order_urls_from_excel(excel_file_path, start_row, num_rows)
        df = read_order_urls_from_excel(excel_file_path)
        failed_downloads = download_pdfs(df, download_dir, browser)
        browser.quit()

        if failed_downloads:
            save_failed_downloads(failed_downloads, os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'failed_downloads.xlsx'))

        browser = initialize_browser(download_dir)
        retry_failed_downloads(download_dir, browser)
        browser.quit()
        
        
        
         # Read final Excel results
        final_excel_path = os.path.join(base_dir, 'cci_orders_with_pdf_names.xlsx')
        final_df = pd.read_excel(final_excel_path)

        # # Remove duplicates based on PDF URL
        # final_df.drop_duplicates(subset=['PDF URL'], keep='last', inplace=True)

        # # Update the PDF Path column to be relative paths
        # final_df['PDF Path'] = final_df['PDF Path'].apply(lambda x: os.path.relpath(x, base_dir))
        
         # Convert the 'Decision Date' to the format 'YYYY-MM-DD'
        final_df ['Decision Date'] = pd.to_datetime(final_df ['Decision Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

        # Add Date_scraped and Updated_date columns
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_df['Date_scraped'] = current_time
        final_df['Updated_date'] = current_time

        # Save the updated DataFrame back to the final Excel file
        final_df.to_excel(final_excel_path, index=False)
        
        
        #store final excel to mysql database
        Store_Data_Mysql.Store_Data_Mysql(final_df)
        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
                
if __name__ == "__main__":
    main()













































# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import pandas as pd
# import time

# # Configure ChromeOptions
# chrome_options = webdriver.ChromeOptions()

# # Initialize the Chrome driver with options
# browser = webdriver.Chrome(options=chrome_options)
# browser.maximize_window()  # Maximize the browser window

# # Open the webpage
# url = 'https://www.cci.gov.in/combination/orders-section43a_44'
# browser.get(url)

# # Function to scrape data from the table on the current page
# def scrape_table():
#     data = []
#     try:
#         time.sleep(5)
#         WebDriverWait(browser, 20).until(
#             EC.presence_of_element_located((By.CSS_SELECTOR, 'table#datatable_ajax tbody tr'))
#         )
#         rows = browser.find_elements(By.CSS_SELECTOR, 'table#datatable_ajax tbody tr')
#         for row in rows:
#             cols = row.find_elements(By.TAG_NAME, 'td')
#             cols_text = [col.text for col in cols]

#             # Extract the anchor tag link
#             anchor_tag = cols[-1].find_element(By.TAG_NAME, 'a')
#             link = anchor_tag.get_attribute('href')
#             cols_text[-1] = link

#             data.append(cols_text)
#     except Exception as e:
#         print(f"Error in scrape_table: {e}")
#     return data

# # Function to handle pagination
# def scrape_all_pages():
#     all_data = []
    
#     # Scrape the first page
#     first_page_data = scrape_table()
#     all_data.extend(first_page_data)
    
#     # Print the data from the first page to the terminal
#     print("First page data:")
#     for row in first_page_data:
#         print("row=======", row)
    
#     while True:
#         try:
#             time.sleep(5)
#             next_button = browser.find_element(By.CSS_SELECTOR, '#datatable_ajax_next a')
#             if 'disabled' in next_button.get_attribute('class'):
#                 break
#             else:
#                 next_button.click()
#                 # Wait for the table to refresh
#                 WebDriverWait(browser, 20).until(
#                     EC.staleness_of(browser.find_element(By.CSS_SELECTOR, 'table#datatable_ajax tbody tr'))
#                 )
#                 # Scrape the data from the new page
#                 all_data.extend(scrape_table())
#         except Exception as e:
#             print(f"Error in scrape_all_pages: {e}")
#             break
#     return all_data

# # Scrape all pages
# data = scrape_all_pages()

# # Convert data to DataFrame
# columns = ['No.',
#            'Combination Registration No.',
#            'Description',
#            'Under Section',
#            'Decision Date',
#            'Order']
# df = pd.DataFrame(data, columns=columns)

# # Save the data to an Excel file
# df.to_excel('cci_orders.xlsx', index=False)

# # Close the browser
# browser.quit()

# print("df======", df)
