from config import cci_config
import sys


connection = cci_config.db_connection()
cursor = connection.cursor()


def insert_log_into_table(log_list):
    print("insert_log_into_table function is called")
    try:
        query = """
            INSERT INTO cci_log (source_name, script_status, data_available, data_scraped, total_record_count, failure_reason, comments, deleted_source, deleted_source_count, source_status)
            VALUES (%(source_name)s, %(script_status)s, %(data_available)s, %(data_scraped)s, %(total_record_count)s, %(failure_reason)s, %(comments)s, %(deleted_source)s, %(deleted_source_count)s, %(source_status)s)
        """
        values = {
            'source_name': cci_config.source_name,
            'script_status': log_list[1] if log_list[1] else None,
            'data_available': log_list[2] if log_list[2] else None,
            'data_scraped': log_list[3] if log_list[3] else None,
            'total_record_count': log_list[4] if log_list[4] else None,
            'failure_reason': log_list[5] if log_list[5] else None,
            'comments': log_list[6] if log_list[6] else None,
            'deleted_source': cci_config.deleted_sources,
            'deleted_source_count': cci_config.deleted_source_count,
            'source_status': cci_config.source_status
        }
        cursor.execute(query, values)
        connection.commit()
    except Exception as e:
        print("Error in insert_log_into_table :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
           