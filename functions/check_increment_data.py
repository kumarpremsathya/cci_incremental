import sys
import traceback
import pandas as pd
from datetime import datetime
from functions import log
from config import cci_config
from sqlalchemy import create_engine
from functions import download_pdf, get_data_count_database, log, send_mail,  updated_data_into_database_table

cursor = cci_config.cursor

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
      
       # Identify updated rows (compare relevant columns)
        for index, row in database_df.iterrows():
            excel_row_desc = excel_df.loc[excel_df["order_link"] == row["order_link"], "description"].values
            excel_row_under_section = excel_df.loc[excel_df["order_link"] == row["order_link"], "under_section"].values
            excel_row_decision_date = excel_df.loc[excel_df["order_link"] == row["order_link"], "decision_date"].values

            if (
                (len(excel_row_desc) > 0 and row["description"] != excel_row_desc[0]) or
                (len(excel_row_under_section) > 0 and row["under_section"] != excel_row_under_section[0]) or
                (len(excel_row_decision_date) > 0 and row["decision_date"] != excel_row_decision_date[0])
            ):
                updated_rows_in_db.append(row)
                updated_rows_in_excel.append(excel_df.loc[excel_df["order_link"] == row["order_link"]].squeeze())

        
        print("updated_rows_in_db===\n\n\n", updated_rows_in_db)
        print("updated_rows_in_excel===\n\n\n", updated_rows_in_excel)
        
        # # Update the database table for updated rows
        updated_data_into_database_table.updated_data_into_database_table(updated_rows_in_db, updated_rows_in_excel)


        print(len(missing_rows_in_db), "missing rows in database")
        print(len(missing_rows_in_excel), "missing rows in Excel")
        # print(missing_rows_in_db,"missing rows in db")

        cci_config.no_data_avaliable = len(missing_rows_in_db)
        cci_config.no_data_scraped = len(missing_rows_in_db)

       
        cci_config.deleted_source_count = len(missing_rows_in_excel)


        if len(missing_rows_in_excel) > 0 and len(missing_rows_in_db) ==0:
            cci_config.log_list[1] = "Success"
            cci_config.log_list[4] = get_data_count_database.get_data_count_database(cci_config.cursor)
            cci_config.log_list[6] = "Some data are deleted in the website"
            log.insert_log_into_table(cci_config.cursor, cci_config.log_list)
            print("log table====", cci_config.log_list)
            cci_config.log_list = [None] * 8
            sys.exit()
        elif len(missing_rows_in_db) == 0:
            cci_config.log_list[1] = "Success"
            cci_config.log_list[4] = get_data_count_database.get_data_count_database(cci_config.cursor)
            cci_config.log_list[6] = "no new data"
            log.insert_log_into_table(cci_config.cursor, cci_config.log_list)
            print("log table====", cci_config.log_list)
            cci_config.log_list = [None] * 8
            sys.exit()



        current_date = datetime.now().strftime("%Y-%m-%d")
        increment_file_name = f"incremental_excel_sheet_{current_date}.xlsx"
        # increment_data_excel_path = fr"C:\Users\mohan.7482\Desktop\CCI\incremental_cci_anti_profiteering\data\incremental_excel_sheet\{increment_file_name}"
        
        increment_data_excel_path = fr"C:\Users\Premkumar.8265\Desktop\cci_project\data\incremental_excel_sheet\{increment_file_name}"
        
        # missing_rows_in_db.to_excel(increment_data_excel_path, index=False)
        pd.DataFrame(missing_rows_in_db).to_excel(increment_data_excel_path, index=False)
        download_pdf.download_pdf(increment_data_excel_path)
        
         
      


    except Exception as e:
        traceback.print_exc()
        cci_config.log_list[1] = "Failure"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database(cci_config.cursor)
        cci_config.log_list[5] = "error in checking in incremental part"
        log.insert_log_into_table(cci_config.cursor, cci_config.log_list)
        print("checking incremental part error:" ,cci_config.log_list)
        send_mail.send_email("cci section 43 orders checking incremental part error",e)
        cci_config.log_list = [None] * 8
        sys.exit()
