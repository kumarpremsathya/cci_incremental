import sys
import traceback
import pandas as pd
import mysql.connector
from config import cci_config
from functions import get_data_count_database, send_mail, log

connection = cci_config.db_connection()
cursor = connection.cursor()


def insert_excel_data_to_mysql(final_excel_sheets_path):
    print("insert_excel_data_to_mysql function is called")

    try:
        df = pd.read_excel(final_excel_sheets_path)

        table_name = "cci_orders_section43a_44"
        # table_name = "cci_orders"
        
        df = df.where(pd.notnull(df), None)
        
        #Convert the 'Decision Date' to the format 'YYYY-MM-DD'
        df['decision_date'] = pd.to_datetime(df['decision_date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
       
        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {table_name} (source_name, combination_reg_no, description, under_section, decision_date, order_link,
                orderpdf_file_name, orderpdf_file_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                source_name = VALUES(source_name),
                combination_reg_no = VALUES(combination_reg_no),
                description = VALUES(description),
                under_section = VALUES(under_section),
                decision_date = VALUES(decision_date),
                orderpdf_file_name = VALUES(orderpdf_file_name),
                orderpdf_file_path = VALUES(orderpdf_file_path);
            """
            
            values = (cci_config.source_name, row["combination_reg_no"], row["description"], row["under_section"],
                      row["decision_date"], row["order_link"], row["orderpdf_file_name"], row["orderpdf_file_path"])
            
            cursor.execute(insert_query, values)

        connection.commit()
      




        cci_config.log_list[1] = "Success"
        cci_config.log_list[2] = cci_config.no_data_avaliable
        cci_config.log_list[3] = cci_config.no_data_scraped
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()
        cci_config.log_list[6] = f"{cci_config.updated_count} rows updated"
        print("log table====", cci_config.log_list)
        log.insert_log_into_table(cci_config.log_list)
        connection.commit()
        connection.close()
        cci_config.log_list = [None] * 8
        print("Data inserted into the database table")


    except Exception as e:
        cci_config.log_list[1] = "Failure"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()
        cci_config.log_list[5] = "error in insert part"
        print("log table====", cci_config.log_list)
        log.insert_log_into_table(cci_config.log_list)
        connection.commit()
        cci_config.log_list = [None] * 8
        traceback.print_exc()
        send_mail.send_email("cci section 43 orders Data inserted into the database error", e)
        
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")
        
        
