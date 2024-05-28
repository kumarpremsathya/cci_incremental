from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
# Configure ChromeOptions
chrome_options = webdriver.ChromeOptions()

# Initialize the Chrome driver with options
browser = webdriver.Chrome(options=chrome_options)
browser.maximize_window()  # Maximize the browser window

# Open the webpage
url = 'https://www.cci.gov.in/combination/orders-section43a_44'
browser.get(url)


# Function to scrape data from the table on the current page
def scrape_table():
    data = []
    WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'table#datatable_ajax tbody tr'))
    )
    rows = browser.find_elements(By.CSS_SELECTOR, 'table#datatable_ajax tbody tr')
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, 'td')
        cols = [col.text for col in cols]
        data.append(cols)
    return data

# Function to handle pagination
def scrape_all_pages():
    all_data = []
    
    # Scrape the first page
    first_page_data = scrape_table()
    all_data.extend(first_page_data)
    
    # Print the data from the first page to the terminal
    print("First page data:")
    for row in first_page_data:
        print("row=======",row)
    
    while True:
        try:
            next_button = browser.find_element(By.CSS_SELECTOR, '#datatable_ajax_next a')
            if 'disabled' in next_button.get_attribute('class'):
                break
            else:
                next_button.click()
                # Wait for the table to refresh
                WebDriverWait(browser, 10).until(
                    EC.staleness_of(browser.find_element(By.CSS_SELECTOR, 'table#datatable_ajax tbody tr'))
                )
                # Scrape the data from the new page
                all_data.extend(scrape_table())
        except Exception as e:
            print(f"Error: {e}")
            break
    return all_data

# Scrape all pages
data = scrape_all_pages()

# Convert data to DataFrame
columns = ['No.',
                'Combination Registration No.',
                'Description',
                'Under Section',
                'Decision Date',
                'Order']
df = pd.DataFrame(data, columns=columns)

# Save the data to a CSV file
df.to_csv('cci_orders.csv', index=False)

# Close the browser
browser.quit()

print("df======", df)
