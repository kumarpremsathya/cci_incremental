from config import cci_config
import sys
import traceback
from functions import download_pdf, get_data_count_database, log, send_mail        
import datetime



# def updated_data_into_database_table(updated_rows_in_db, updated_rows_in_excel):
#     try:
#         table_name = "cci_orders_section43a_44"
#         updated_count = 0

#         for db_row, excel_row in zip(updated_rows_in_db, updated_rows_in_excel):
#             if (
#                 db_row["description"] != excel_row["description"]
#                 or db_row["under_section"] != excel_row["under_section"]
#                 or db_row["decision_date"] != excel_row["decision_date"]
#             ):
#                 current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                 update_query = f"""
#                     UPDATE {table_name}
#                     SET description = %s, under_section = %s, decision_date = %s, updated_date = %s
#                     WHERE order_link = %s
#                 """
#                 values = (
#                     excel_row["description"],
#                     excel_row["under_section"],
#                     excel_row["decision_date"],
#                     current_timestamp,
#                     db_row["order_link"],
#                 )
#                 cci_config.cursor.execute(update_query, values)
#                 updated_count += 1

#         cci_config.connection.commit()

#         if updated_count > 0:
#             cci_config.log_list[6] = f"{updated_count} rows updated"
#         else:
#             cci_config.log_list[6] = "no new data"

#     except Exception as e:
#         cci_config.log_list[1] = "Failure"
#         cci_config.log_list[4] = get_data_count_database.get_data_count_database(cci_config.cursor)
#         cci_config.log_list[5] = "Error updating database table"
#         log.insert_log_into_table(cci_config.cursor, cci_config.log_list)
#         cci_config.connection.commit()
#         cci_config.log_list = [None] * 8
#         traceback.print_exc()
#         send_mail.send_email("cci section 43 orders Error updating database table", e)
#         sys.exit("Script error")

connection = cci_config.db_connection()
cursor = connection.cursor()

def updated_data_into_database_table(updated_rows_in_db, updated_rows_in_excel):
    print("updated_data_into_database_table function is called")
    try:
        table_name = "cci_orders_section43a_44"
        updated_count = 0
         
        for db_row, excel_row in zip(updated_rows_in_db, updated_rows_in_excel):
            current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Check if each relevant field has changed and build the update query accordingly
            fields_to_update = []
            values = []

            if db_row["description"] != excel_row["description"]:
                fields_to_update.append("description = %s")
                values.append(excel_row["description"])

            if db_row["under_section"] != excel_row["under_section"]:
                fields_to_update.append("under_section = %s")
                values.append(excel_row["under_section"])

            if db_row["decision_date"] != excel_row["decision_date"]:
                fields_to_update.append("decision_date = %s")
                values.append(excel_row["decision_date"])

            if fields_to_update:
                fields_to_update.append("updated_date = %s")
                values.append(current_timestamp)
                values.append(db_row["order_link"])

                update_query = f"""
                    UPDATE {table_name}
                    SET {", ".join(fields_to_update)}
                    WHERE order_link = %s
                """
                cursor.execute(update_query, values)
                updated_count += 1
          
        
            
        connection.commit()
        print("updated_data_into_database_table finished")
        return updated_count
    
        
    except Exception as e:
        cci_config.log_list[1] = "Failure"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()
        cci_config.log_list[5] = "Error updating database table"
        log.insert_log_into_table(cci_config.log_list)
        cci_config.log_list = [None] * 8
        traceback.print_exc()
        send_mail.send_email("cci section 43 orders Error updating database table", e)
        sys.exit("Script error")