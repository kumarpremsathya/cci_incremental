from config import cci_config
import sys
import traceback
from functions import download_pdf, get_data_count_database, log, send_mail

def updated_data_into_database_table(updated_rows_in_db, updated_rows_in_excel):
    print("updated_data_into_database_table function is called")
    try:
        table_name = "cci_orders_section43a_44"

        for db_row, excel_row in zip(updated_rows_in_db, updated_rows_in_excel):
            update_query = f"""
                UPDATE {table_name}
                SET description = %s, under_section = %s, decision_date = %s
                WHERE order_link = %s
            """
            values = (
                excel_row["description"],
                excel_row["under_section"],
                excel_row["decision_date"],
                db_row["order_link"],
            )
            cci_config.cursor.execute(update_query, values)

        cci_config.connection.commit()
        print("updated_data_into_database_table finished")
        
    except Exception as e:
        cci_config.log_list[1] = "Failure"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database(cci_config.cursor)
        cci_config.log_list[5] = "Error updating database table"
        log.insert_log_into_table(cci_config.cursor, cci_config.log_list)
        print("Error updating database table part error:" ,cci_config.log_list)
        cci_config.connection.commit()
        cci_config.log_list = [None] * 8
        traceback.print_exc()
        send_mail.send_email("cci section 43 orders Error updating database table", e)
        sys.exit("Script error")