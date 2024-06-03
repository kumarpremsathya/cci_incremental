import sys
import traceback
import pandas as pd
from datetime import datetime
from functions import log
from config import cci_config
from sqlalchemy import create_engine
from functions import download_pdf, get_data_count_database, log, send_mail,  updated_data_into_database_table
from sqlalchemy.sql import text

connection = cci_config.db_connection()
cursor = connection.cursor()

def check_increment_data(excel_path):
    print("check_increment_data function is called")
    try:
        # database_uri = f'mysql://{cci_config.user}:{cci_config.password}@{cci_config.host}/{cci_config.database}?auth_plugin={cci_config.auth_plugin}'
        database_uri = f'mysql://{cci_config.user}:{cci_config.password}@{cci_config.host}/{cci_config.database}'

        engine = create_engine(database_uri)
        query = "SELECT * FROM cci_orders_section43a_44"
        # query = "SELECT * FROM cci_orders"
       
        
        database_df = pd.read_sql(query, con=engine)

        excel_df = pd.read_excel(excel_path)
        print("excel_df ====", excel_df.to_string)
       

        missing_rows_in_db = []
        missing_rows_in_excel = []

        for index, row in database_df.iterrows():
            if row["order_link"] not in excel_df["order_link"].values:
                missing_rows_in_excel.append(row)

        for index, row in excel_df.iterrows():
            if row["order_link"] not in database_df["order_link"].values:
                missing_rows_in_db.append(row)

        for row in missing_rows_in_excel:
            print(missing_rows_in_excel)
            cci_config.deleted_sources += row["orderpdf_file_name"] + ", "
            

        # Convert missing_rows_in_excel to a DataFrame
        missing_rows_in_excel_df = pd.DataFrame(missing_rows_in_excel)

        # Update the flag column for the deleted sources in database as deleted
        # for index, row in missing_rows_in_excel_df.iterrows():
        #     order_link = row['order_link']
        #     update_query = f"""
        #         UPDATE cci_orders_section43a_44
        #         SET flag = 'deleted'
        #         WHERE order_link = '{order_link}'
        #     """
        #     cursor.execute(update_query)
        #     cci_config.connection.commit()
        

        print("deleted sources pdf in excel", cci_config.deleted_sources)
         
        updated_rows_in_db = []
        updated_rows_in_excel = []

        database_df = database_df.reset_index(drop=True)
        excel_df = excel_df.reset_index(drop=True)

        for index, db_row in database_df.iterrows():
            excel_row = excel_df[excel_df["order_link"] == db_row["order_link"]]
            if not excel_row.empty:
                excel_row = excel_row.iloc[0]

                # Normalizing the data types for accurate comparison
                db_row_normalized = db_row.copy()
                excel_row_normalized = excel_row.copy()

                db_row_normalized["decision_date"] = pd.to_datetime(db_row_normalized["decision_date"], dayfirst=True, errors='coerce')
                excel_row_normalized["decision_date"] = pd.to_datetime(excel_row_normalized["decision_date"], dayfirst=True, errors='coerce')

                # Ensuring that all fields are string type for comparison
                for col in ["combination_reg_no", "description", "under_section", "order_link"]:
                    db_row_normalized[col] = db_row_normalized[col] if pd.isnull(db_row_normalized[col]) else str(db_row_normalized[col]).strip()
                    excel_row_normalized[col] = excel_row_normalized[col] if pd.isnull(excel_row_normalized[col]) else str(excel_row_normalized[col]).strip()

                # Comparison logic
                significant_difference = False
                for column in ["combination_reg_no", "description", "under_section", "decision_date"]:
                    db_value = db_row_normalized[column]
                    excel_value = excel_row_normalized[column]

                    if pd.isnull(db_value) and pd.isnull(excel_value):
                        continue

                    if isinstance(db_value, str):
                        db_value = db_value.strip()

                    if isinstance(excel_value, str):
                        excel_value = excel_value.strip()

                    if db_value != excel_value:
                        print(f"Difference found in column: {column}, db_value: {db_value}, excel_value: {excel_value}")
                        significant_difference = True
                        break

                if significant_difference:
                    updated_rows_in_db.append(db_row)
                    updated_rows_in_excel.append(excel_row)
                    
                    
        
        print("updated_rows_in_db===\n\n\n", updated_rows_in_db)
        print("updated_rows_in_excel===\n\n\n", updated_rows_in_excel)
        
            
        if len(updated_rows_in_db) > 0:
       
            cci_config.updated_count =updated_data_into_database_table.updated_data_into_database_table(updated_rows_in_db, updated_rows_in_excel)
            print("updated count========",cci_config.updated_count )
            if cci_config.updated_count > 0:
                update_db = f"{cci_config.updated_count} rows updated"
            else:
                update_db = "0 rows updated"
        else:
                update_db = "0 rows updated"
        
        print("updated count========",cci_config.updated_count) 
        


        print(len(missing_rows_in_db), "missing rows in database")
        print(len(missing_rows_in_excel), "missing rows in Excel")
        # print(missing_rows_in_db,"missing rows in db")

        cci_config.no_data_avaliable = len(missing_rows_in_db)
        cci_config.no_data_scraped = len(missing_rows_in_db)

       
        cci_config.deleted_source_count = len(missing_rows_in_excel)


        if len(missing_rows_in_excel) > 0 and len(missing_rows_in_db) ==0:
            cci_config.log_list[1] = "Success"
            cci_config.log_list[4] = get_data_count_database.get_data_count_database()
            cci_config.log_list[6] = f"{update_db}, Some data are deleted in the website"
            log.insert_log_into_table(cci_config.log_list)
            print("log table====", cci_config.log_list)
            cci_config.log_list = [None] * 8
            sys.exit()
            
        
            
        elif len(missing_rows_in_db) == 0:
            cci_config.log_list[1] = "Success"
            cci_config.log_list[4] = get_data_count_database.get_data_count_database()
            cci_config.log_list[6] = f"{update_db}, no new data"
            log.insert_log_into_table(cci_config.log_list)
            print("log table====", cci_config.log_list)
            cci_config.log_list = [None] * 8
            sys.exit()
        

       
        current_date = datetime.now().strftime("%Y-%m-%d")
        increment_file_name = f"incremental_excel_sheet_{current_date}.xlsx"
        
        increment_data_excel_path = fr"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\data\incremental_excel_sheet\{increment_file_name}"
        
        # missing_rows_in_db.to_excel(increment_data_excel_path, index=False)
        pd.DataFrame(missing_rows_in_db).to_excel(increment_data_excel_path, index=False)
        download_pdf.download_pdf(increment_data_excel_path)
        
         

    except Exception as e:
        traceback.print_exc()
        cci_config.log_list[1] = "Failure"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()
        cci_config.log_list[5] = "error in checking in incremental part"
        log.insert_log_into_table(cci_config.log_list)
        print("checking incremental part error:" ,cci_config.log_list)
        send_mail.send_email("cci section 43 orders checking incremental part error",e)
        cci_config.log_list = [None] * 8
        
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit()
