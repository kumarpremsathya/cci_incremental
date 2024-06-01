import sys
import traceback
from config import cci_config
from functions import extract_all_data_in_website, log, get_data_count_database, check_increment_data


def main():
    print("main function is called")
    if cci_config.source_status == "Active":
        extract_all_data_in_website.extract_all_data_in_website()
        
        # first_excel_sheet_name =f"first_excel_sheet_{cci_config.current_date}.xlsx"
        # first_exceL_sheet_path = rf"C:\Users\Premkumar.8265\Desktop\cci_project\cci_incremental\data\first_excel_sheet\{first_excel_sheet_name}"
        # check_increment_data.check_increment_data(first_exceL_sheet_path)
        print("finished")


    elif cci_config.source_status == "Hibernated":
        cci_config.log_list[1] = "not run"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()
        print(cci_config.log_list)
        log.insert_log_into_table(cci_config.log_list)

        
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        
        
    elif cci_config.source_status == "Inactive":
        cci_config.log_list[1] = "not run"
        cci_config.log_list[4] = get_data_count_database.get_data_count_database()
        print(cci_config.log_list)
        cci_config.log_list = [None] * 8
        traceback.print_exc()

        print(cci_config.log_list)
        log.insert_log_into_table(cci_config.log_list)
        
        print(cci_config.log_list)
        cci_config.log_list = [None] * 8
        traceback.print_exc()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error occurred at line {exc_tb.tb_lineno}:")
        print(f"Exception Type: {exc_type}")
        print(f"Exception Object: {exc_obj}")
        print(f"Traceback: {exc_tb}")
        sys.exit("script error")



if __name__ == "__main__":
    main()
