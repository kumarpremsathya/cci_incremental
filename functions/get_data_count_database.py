def get_data_count_database(cursor):
    print("get_data_count_database function is called")
    cursor.execute("SELECT COUNT(*) FROM cci_orders;")
    # print("count from database",cursor.fetchone()[0])
    # return cursor.fetchone()[0]

    result = cursor.fetchone()
    print("Result from database query:", result)
    if result:
        return result[0]
    else:
        raise ValueError("Query did not return any results")