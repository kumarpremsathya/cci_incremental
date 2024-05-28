from config import cci_config

def insert_log_into_table(cursor, log_list):
    query = """
        INSERT INTO cci_43_log (source_name, script_status, data_available, data_scraped, total_record_count, failure_reason, comments, deleted_source, deleted_source_count, source_status)
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
    cci_config.connection.commit()