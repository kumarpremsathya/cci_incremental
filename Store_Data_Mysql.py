import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime

# MySQL database credentials
host="localhost",
user="root",
password="root",
database="cci_43"



# Path to the Excel file
# excel_file_path = r"C:\\Users\\Premkumar.8265\\Desktop\\cci project\\cci_orders_with_pdf_names.xlsx"


# def read_final_excel():
   

#     # # Check if the Excel file exists
#     # if not os.path.exists(excel_file_path):
#     #         # If the Excel file doesn't exist, create an empty DataFrame with the required columns
#     #         df = pd.DataFrame(columns=['No.', 'Combination Registration No.', 'Description', 'Under Section',
#     #                                    'Decision Date', 'PDF URL', 'PDF Name', 'PDF Path'])
#     #         print("if block is excuted",df.to_string)
#     # else:
#     #     # Read the Excel file into a DataFrame
#     #     pass
#     #     df = pd.read_excel(excel_file_path)
#     #     print("else block is excuted", df.to_string)
    
        
#     # Reading the Excel file into a pandas DataFrame
#     df = pd.read_excel(excel_file_path)


#     # Remove duplicates based on PDF URL
#     df.drop_duplicates(subset=['PDF URL'], keep='last', inplace=True)


#     # Convert the 'Decision Date' to the format 'YYYY-MM-DD'
#     df['Decision Date'] = pd.to_datetime(df['Decision Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    
#     return df
    



# Connect to MySQL database
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="cci_43"
        )
        print("Connected to MySQL database")
        return conn
    except mysql.connector.Error as e:
        print("Error connecting to MySQL database:", e)
        return None


# Function to create the table in the MySQL database
def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cci_orders (
        `No` INT,
        `Combination_Registration_No` VARCHAR(255),
        `Description` TEXT,
        `Under_Section` VARCHAR(50),
        `Decision_Date` DATE,
        `PDF_URL` TEXT,
        `PDF_Name` VARCHAR(255),
        `PDF_Path` TEXT,
        `Date_scraped` TIMESTAMP,
        `Updated_date` TIMESTAMP,
        UNIQUE (`PDF_URL`(255)),
        UNIQUE (`PDF_Name`)
    );
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully")
    except Error as e:
        print(f"Error: '{e}'")
    finally:
        cursor.close()

# Function to insert DataFrame data into the MySQL database
def insert_data(connection, final_df):
    # insert_query = """
    # INSERT INTO cci_orders (`No.`, `Combination Registration No.`, `Description`, `Under Section`, `Decision Date`, `PDF URL`, `PDF Name`, `PDF Path`)
    # VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    # """
    
    
    
    insert_query = """
    INSERT INTO cci_orders (`No`, `Combination_Registration_No`, `Description`, `Under_Section`, `Decision_Date`, `PDF_URL`, `PDF_Name`, `PDF_Path`, `Date_scraped`, `Updated_date`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `Combination_Registration_No` = VALUES(`Combination_Registration_No`),
        `Description` = VALUES(`Description`),
        `Under_Section` = VALUES(`Under_Section`),
        `Decision_Date` = VALUES(`Decision_Date`),
        `PDF_Path` = VALUES(`PDF_Path`),
        `Updated_date` = VALUES(`Updated_date`);
    """
    
    cursor = connection.cursor()
    try:
        for _, row in final_df.iterrows():
            # cursor.execute(insert_query, tuple(row))
            cursor.execute(insert_query, (
                row['No.'],
                row['Combination Registration No.'],
                row['Description'],
                row['Under Section'],
                row['Decision Date'],
                row['PDF URL'],
                row['PDF Name'],
                row['PDF Path'],
                row['Date_scraped'],
                row['Updated_date']
            ))

        connection.commit()
        print("Data inserted successfully")
    except Error as e:
        print(f"Error: '{e}'")
    finally:
        cursor.close()



def Store_Data_Mysql(final_df):
    
    connection = connect_to_mysql()
    if connection is not None:
        # df=read_final_excel()
        # print("read_final_excel:\n", df.to_string())
        create_table(connection)
        insert_data(connection, final_df)
        connection.close()
        print("MySQL connection closed")

