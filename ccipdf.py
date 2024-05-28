from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import time
import shutil

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
def read_order_urls_from_excel(excel_file_path, start_row, num_rows):
    # Read the Excel file
    df = pd.read_excel(excel_file_path)
    
    # Rename columns to ensure 'Order' column is present
    df.rename(columns={'Order': 'PDF URL'}, inplace=True)
    
    # Select only the specified range of rows
    df = df.iloc[start_row:start_row + num_rows]
    
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
            decision_date = pd.to_datetime(row['Decision Date'], format='%d/%m/%Y')  # Ensure decision_date is set here
            print(f"pdf_url=== {pdf_url}")
            retries = 2
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
                    download_timeout = 30
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
                    
                    print("exceuted correctly1")
                    while not os.path.exists(download_path):
                        time.sleep(1)
                        if time.time() - start_time > download_timeout:
                            raise Exception(f"Download timed out for {pdf_name}")

                    decision_date = pd.to_datetime(row['Decision Date'], format='%d/%m/%Y')
                    year_folder = os.path.join(download_dir, str(decision_date.year))
                    month_folder = os.path.join(year_folder, decision_date.strftime('%b'))
                    if not os.path.exists(month_folder):
                        os.makedirs(month_folder)
                    
                    print("exceuted correctly 2")
                    destination_path = os.path.join(month_folder, pdf_name)
                    shutil.move(download_path, destination_path)
                    df.at[index, 'PDF Path'] = destination_path
                    
                    print("exceuted correctly 3")
                    print(f"Downloaded: {pdf_name} and stored in folder {month_folder}")
                    df.to_excel(os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders_with_pdf_names.xlsx'), index=False)
                    break
                except Exception as download_error:
                    print(f"Attempt {attempt + 1} failed for {pdf_url}: {download_error}")
                    if attempt == retries - 1:
                        failed_downloads.append((pdf_url, decision_date.strftime('%d/%m/%Y')))
        except Exception as e:
            print(f"Error for row {index}: {e}")

    return df, failed_downloads


def retry_failed_downloads(df,download_dir, browser):
    print("retry_failed_downloads function is called.")
    failed_downloads_file = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'failed_downloads.txt')
    excel_file_path = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders_with_pdf_names.xlsx')

    if not os.path.exists(failed_downloads_file):
        print("No failed downloads to retry.")
        return
    
    # Check if the Excel file exists
    if not os.path.exists(excel_file_path):
        # If the Excel file doesn't exist, create an empty DataFrame with the required columns
        df = pd.DataFrame(columns=['No.', 'Combination Registration No.', 'Description', 'Under Section',
                                   'Decision Date', 'PDF URL', 'PDF Name', 'PDF Path'])
    else:
        # Read the Excel file into a DataFrame
        df = pd.read_excel(excel_file_path)
    
    
    with open(failed_downloads_file, 'r') as f:
        failed_urls = f.readlines()

    failed_urls = [url.strip() for url in failed_urls]

    for line in failed_urls:
        try:
            pdf_url, decision_date_str = eval(line)  # Use eval to unpack the tuple correctly
            decision_date = pd.to_datetime(decision_date_str, format='%d/%m/%Y')
            retries = 3
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
                    download_timeout = 30
                    start_time = time.time()

                    while not os.path.exists(download_path):
                        time.sleep(1)
                        if time.time() - start_time > download_timeout:
                            raise Exception(f"Download timed out for {pdf_name}")

                    
                    year_folder = os.path.join(download_dir, str(decision_date.year))
                    month_folder = os.path.join(year_folder, decision_date.strftime('%b'))
                    if not os.path.exists(month_folder):
                        os.makedirs(month_folder)

                    destination_path = os.path.join(month_folder, pdf_name)
                    shutil.move(download_path, destination_path)

                    print(f"Downloaded: {pdf_name} and stored in folder {month_folder}")
                    
                    # Update the DataFrame with the new PDF path
                    # df.loc[df['PDF URL'] == pdf_url, 'PDF Path'] = destination_path
                    # df.to_excel(excel_file_path, index=False)
                    
                    
                    # Update the DataFrame with the new PDF path
                    for index, row in df.iterrows():
                        if row['PDF URL'] == pdf_url:
                            df.at[index, 'PDF Path'] = destination_path

                    # Save the updated DataFrame to the Excel file
                    df.to_excel(excel_file_path, index=False)
                    print("df=====", df)
                    
                    break
                except Exception as download_error:
                    print(f"Attempt {attempt + 1} failed for {pdf_url}: {download_error}")
        except Exception as e:
            print(f"Error retrying {pdf_url}: {e}")



def main():
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", "downloaded_pdfs"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(options=chrome_options)
    browser.maximize_window()

    excel_file_path = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'cci_orders.xlsx')
    download_dir = os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", "downloaded_pdfs")
    start_row = 16
    num_rows = 1

    df = read_order_urls_from_excel(excel_file_path, start_row, num_rows)
    failed_downloads = download_pdfs(df, download_dir, browser)
    browser.quit()

    if failed_downloads:
        with open(os.path.join("C:\\Users\\Premkumar.8265\\Desktop\\cci project", 'failed_downloads.txt'), 'w') as f:
            for url in failed_downloads:
                f.write(f"{url}\n")

    # Retry failed downloads
    browser = webdriver.Chrome(options=chrome_options)
    retry_failed_downloads(df, download_dir, browser)
    print("retry_failed_downloads", retry_failed_downloads)
    browser.quit()

if __name__ == "__main__":
    main()
