g_list)
        log.insert_log_into_table(cci_config.cursor, cci_config.log_list)
        cci_config.connection.commit()
        print(cci_config.log_list)
        cci_config.log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")



if __name__ == "__main__":
    main()
