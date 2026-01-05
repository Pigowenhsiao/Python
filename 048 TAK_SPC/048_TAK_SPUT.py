#!/usr/bin/env python  # Specifies the interpreter to execute this program
# -*- coding: utf-8 -*-  # Sets the encoding of this file to UTF-8

"""  
This program's functionality:
1. Reads all .ini files, processes Excel file data according to the configuration, and generates XML files. # Explains the program's purpose
2. Running records and error logs are output by the custom module Log. # Explains the logging method

Dependent Modules:
- Log, SQL, Check, Convert_Date, Row_Number_Func (all in ../MyModule) # Lists the dependent custom modules
"""  # Multi-line comment: Program description

import os  # Imports the os module for operating system related operations
import sys  # Imports the sys module to interact with the Python interpreter
import glob  # Imports the glob module for file path matching
import shutil  # Imports the shutil module for file copying and moving operations
import logging  # Imports the logging module for logging
import pandas as pd  # Imports the pandas module, aliased as pd, for data processing
import random 
from configparser import ConfigParser, NoSectionError, NoOptionError  # Imports classes for configuration parsing from the configparser module
from datetime import datetime, timedelta, date  # Imports date and time related classes from the datetime module

sys.path.append('../MyModule')  # Adds ../MyModule to the system module search path
import Log  # Imports the custom Log module for logging
import SQL  # Imports the custom SQL module for database operations
import Check  # Imports the custom Check module
import Convert_Date  # Imports the custom Convert_Date module
import Row_Number_Func  # Imports the custom Row_Number_Func module for handling row numbers

global_log_file = None  # Defines a global variable global_log_file, initialized to None

def setup_logging(log_file_path: str) -> None:  # Defines the setup_logging function to set the log format and file
    """Sets the format and file for logging."""  # Function description: Sets the log output format and file to write to
    try:  # Tries to execute the following code
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,  # Sets the log file and level
                            format='%(asctime)s - %(levelname)s - %(message)s')  # Sets the log output format
    except OSError as e:  # Catches OSError exception
        print(f"Error setting up log file {log_file_path}: {e}")  # Prints the error message to the console
        raise  # Re-raises the exception

def update_running_rec(running_rec_path: str, end_date: datetime) -> None:  # Defines the update_running_rec function to update the running record file
    """Updates the running record file."""  # Function description: Writes the latest end date to the running record file
    try:  # Tries to execute the following code
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # Opens the running record file in write mode
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))  # Formats the date and writes it to the file
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} updated with end date {end_date}")  # Logs the successful update message
    except Exception as e:  # Catches all exceptions
        Log.Log_Error(global_log_file, f"Error updating running record file {running_rec_path}: {e}")  # Logs the error message

def ensure_running_rec_exists_and_update(running_rec_path: str, end_date: datetime) -> None:  # Defines the ensure_running_rec_exists_and_update function to ensure the running record file exists and update it
    """Creates and updates the running record file if it does not exist."""  # Function description: Checks if the running record file exists, creates it if not, then updates it
    try:  # Tries to execute the following code
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # Opens (or creates) the running record file in write mode
            f.write(end_date.strftime('%Y-%m-%d %H:%M:%S'))  # Writes the end date to the file
        Log.Log_Info(global_log_file, f"Running record file {running_rec_path} confirmed and updated with end date {end_date}")  # Logs the successful update message
    except Exception as e:  # Catches all exceptions
        Log.Log_Error(global_log_file, f"Error processing running record file {running_rec_path}: {e}")  # Logs the error message

def read_running_rec(running_rec_path: str) -> datetime:  # Defines the read_running_rec function to read the running record file
    """
    Reads the last run record.
    If the file does not exist or the content is invalid, it returns the date 30 days ago.
    """  # Function description: Tries to read the run record file, returns a default date on failure
    if not os.path.exists(running_rec_path):  # If the file does not exist
        with open(running_rec_path, 'w', encoding='utf-8') as f:  # Creates an empty file
            f.write('')  # Writes an empty string
        return datetime.today() - timedelta(days= 30 )  # Returns the date 30 days ago
    try:  # Tries to read the file content
        with open(running_rec_path, 'r', encoding='utf-8') as f:  # Opens the file in read mode
            content = f.read().strip()  # Reads the content and removes leading/trailing whitespace
            if content:  # If the content is not empty
                last_run_date = pd.to_datetime(content, errors='coerce')  # Converts to datetime format
                if pd.isnull(last_run_date):  # If the conversion result is invalid
                    return datetime.today() - timedelta(days=30)  # Returns the date 30 days ago
                return last_run_date  # Returns the converted date
            else:  # If the content is empty
                return datetime.today() - timedelta(days=30)  # Returns the date 30 days ago
    except Exception as e:  # Catches all exceptions
        Log.Log_Error(global_log_file, f"Error reading running record file {running_rec_path}: {e}")  # Logs the error message
        return datetime.today() - timedelta(days=30)  # Returns the date 30 days ago

def process_excel_file(file_path: str, sheet_name: str, data_columns, running_rec: str,
                       output_path: str, fields: dict, site: str, product_family: str,
                       operation: str, Test_Station: str, config: ConfigParser) -> None:  # Defines the process_excel_file function to process Excel files
    """Processes Excel files, reads data, transforms it, and generates XML files."""  # Function description: Reads and processes Excel data based on configuration, then generates XML files
    Log.Log_Info(global_log_file, f"Processing Excel File: {file_path}")  # Logs the start of Excel file processing
    Excel_file_list = []  # Initializes an empty list to store files and their modification times
    for file in glob.glob(file_path):  # Iterates through all files matching file_path
        if '$' not in file:  # Filters out temporary files containing '$' in their names
            dt = datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d %H:%M:%S")  # Gets and formats the file's modification time
            Excel_file_list.append([file, dt])  # Adds the file path and modification time to the list
    if not Excel_file_list:  # If the list is empty
        Log.Log_Error(global_log_file, f"Excel file not found: {file_path}")  # Logs an error
        return  # Exits the function
    Excel_file_list = sorted(Excel_file_list, key=lambda x: x[1], reverse=True)  # Sorts files by modification time (newest first)
    Excel_File = Excel_file_list[0][0]  # Gets the path and name of the latest file
    
    try:  # Tries to read Excel data
        # Reads Excel data, skipping the first 1000 rows, and only reading specified columns
        df = pd.read_excel(Excel_File, header=None, sheet_name=sheet_name, usecols=data_columns, skiprows=1000)
        df['key_SORTNUMBER'] = df.index + 1000  # Adds a 'key_SORTNUMBER' column with the value of index + 1000

    except Exception as e:  # If reading fails
        Log.Log_Error(global_log_file, f"Error reading Excel file {file_path}: {e}")  # Logs an error
        return  # Exits the function
    df.columns = range(df.shape[1])  # Renames DataFrame columns to 0, 1, 2, ...     
    df = df.dropna(subset=[2])  # Deletes rows where the third column (index 2) is NaN

    if not os.path.exists(output_path):  # If the output directory does not exist
        os.makedirs(output_path)  # Creates the output directory
        
    one_month_ago = read_running_rec(running_rec)  # Gets the date from 30 days ago based on the running record
    if 'key_Start_Date_Time' in fields:  # If the configuration contains the key_Start_Date_Time field
        start_date_col = int(fields['key_Start_Date_Time'][0])  # Gets the column number for this field
        #print(start_date_col,df[start_date_col])  # Prints the column number for this field
        running_date = config.get('Basic_info', 'Running_date')  # Gets the Running_date value from the ini file
        one_month_ago = datetime.today() - timedelta(days=int(running_date))  # Calculates the date from `running_date` days ago
        df = df[df[start_date_col].apply(pd.to_datetime, errors='coerce') >= one_month_ago]  # Filters for rows with dates greater than or equal to `one_month_ago`
        df[start_date_col] = df[start_date_col].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S'))  # Formats the date in this column
    else:  # If the field is not in the configuration
        Log.Log_Error(global_log_file, "key_Start_Date_Time not found in fields configuration")  # Logs an error
        # Extract values from the DataFrame based on the fields configuration

    if 'key_Start_Date_Time' in fields and 'key_END_Date_Time' in fields and 'key_Operator1' in fields and \
       'key_Operator2' in fields and 'key_Serial_Number' in fields and 'key_Material_Type' in fields and \
       'key_Coating_Type' in fields and 'key_Reflectivity' in fields:
        extracted_values = {
            "key_Start_Date_Time": df[int(fields['key_Start_Date_Time'][0])].tolist(),
            "key_END_Date_Time": df[int(fields['key_END_Date_Time'][0])].tolist(),
            "key_Operator1": df[int(fields['key_Operator1'][0])].tolist(),
            "key_Operator2": df[int(fields['key_Operator2'][0])].tolist(),
            "key_Serial_Number": df[int(fields['key_Serial_Number'][0])].tolist(),
            "key_Material_Type": df[int(fields['key_Material_Type'][0])].tolist(),
            "key_Coating_Type": df[int(fields['key_Coating_Type'][0])].tolist(),
            "key_Reflectivity": df[int(fields['key_Reflectivity'][0])].tolist(),
            "key_SORTNUMBER": df[int(fields['key_SORTNUMBER'][0])].tolist()
        }
        # Ensures that the values in extracted_values are valid column indices

        valid_columns = [int(fields[key][0]) for key in extracted_values.keys() if key in fields]
        df1 = df.iloc[:, valid_columns].copy()  # Copies the DataFrame based on valid column indices
        df1.columns = list(extracted_values.keys())  # Sets the column names to the keys of extracted_values
    else:
        Log.Log_Error(global_log_file, "Required fields are missing in the fields configuration")  # Logs an error
        return
    df1 = df1.reset_index(drop=True)

    # Split the 'key_Serial_Number' column by '/' and generate new rows
    new_rows = []
    for index, row in df1.iterrows():
        serial_numbers = str(row['key_Serial_Number']).split('/')  # Split by '/'
        for serial in serial_numbers:
            serial = serial.strip()  # Remove leading/trailing whitespace
            serial = serial.split()[0]  # Keep only the first part before any whitespace
            if not serial:  # Skip empty serial numbers
                continue
            new_row = row.copy()  # Copy the original row
            new_row['key_Serial_Number'] = serial  # Replace with the split serial number
            new_rows.append(new_row)  # Add the new row to the list

    # Create a new DataFrame with the expanded rows
    df1 = pd.DataFrame(new_rows).reset_index(drop=True)
    df1['Part_Number'] = None  # Initializes the Part_Number column to None
    df1['Chip_Part_Number'] = None  # Initializes the Chip_Part_Number column to None
    df1['COB_Part_Number'] = None  # Initializes the COB_Part_Number column to None
    for index, row in df1.iterrows():  # Iterates through each row of df1
        key_Material_Type = str(row['key_Material_Type'])  # Gets the value of key_Material_Type
        if "QJ-30150" in key_Material_Type:
            part_number = "XQJ-30150"
            chip_part_number = "1000047352A"
            cob_part_number = "1000047353A"
        elif "QJ-30115" in key_Material_Type:
            part_number = "XQJ-30115-P"
            chip_part_number = "1000034198A"
            cob_part_number = "1000034812A"
        else:
            part_number = None
            chip_part_number = None
            cob_part_number = None

        # Updates the column values
        df1.loc[index, 'Part_Number'] = part_number
        df1.loc[index, 'Chip_Part_Number'] = chip_part_number
        df1.loc[index, 'COB_Part_Number'] = cob_part_number
        # Adds corresponding columns and puts values into the DataFrame
    # Drop rows with any NaN values in df1
    df1 = df1.dropna().reset_index(drop=True)
    # Save df1 to a CSV file in the specified output path

    df1.rename(columns={'key_Start_Date_Time': 'Start_Date_Time'}, inplace=True)
    df1['Start_Date_Time'] = pd.to_datetime(df1['Start_Date_Time'], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
    df1.rename(columns={'key_END_Date_Time': 'End_Date_Time'}, inplace=True)
    df1['End_Date_Time'] = pd.to_datetime(df1['End_Date_Time'], errors='coerce').dt.strftime('%Y/%m/%d %H:%M:%S')
    cvd_tool_value = config.get('Basic_info', 'CVD_Tool')  # Read the CVD_Tool value from the ini file
    df1['CVD_Tool'] = cvd_tool_value  # Add the column and assign the value
    df1.rename(columns={'key_Operator1': 'Operator'}, inplace=True)
    df1.rename(columns={'key_Serial_Number': 'Serial_Number'}, inplace=True)
    # Renames the key_Material_Type column to Material_Type
    df1.rename(columns={'key_Material_Type': 'Material_Type'}, inplace=True)
    # Removes line break characters (\n, \r, etc.) from the Material_Type column
    df1['Material_Type'] = df1['Material_Type'].astype(str).str.replace(r'[\r\n]+', '', regex=True)
    df1.rename(columns={'key_Coating_Type': 'Coating_Type'}, inplace=True)
    df1.rename(columns={'key_Reflectivity': 'Reflectivity'}, inplace=True)
    df1.rename(columns={'key_SORTNUMBER': 'SORTNUMBER'}, inplace=True)
    
    current_time = datetime.now().strftime("%Y%m%d%H%M")  # Gets the current time and formats it as YYYYMMDDHHMM
    random_suffix = f"{random.randint(0, 60):02}"  # Generate a random number between 0 and 60, formatted as two digits
    current_time = current_time + random_suffix  # Append the random number to the current_time string
    csv_output_path = os.path.join(config.get('Paths', 'CSV_path'), f"TAK_SPC_{current_time}.csv")
    df1.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
    Log.Log_Info(global_log_file, f"CSV file saved at {csv_output_path}")
    generate_xml(output_path, site, product_family, operation, Test_Station, current_time, config,csv_output_path)  # Calls generate_xml to generate the XML file
    Log.Log_Info(global_log_file, "Write the next starting line number")  # Logs the message for the next starting line number

def generate_xml(output_path: str, site: str, product_family: str,
                 operation: str, Test_Station: str, current_time: str, config: ConfigParser, csv_output_path:str ) -> None:  # Defines the generate_xml function to generate XML files
    """Generates an XML file."""  # Function description: Generates an XML file based on the passed data
    from datetime import datetime  # Imports the datetime module
    # Store current time in two different formats
    current_time_standard = datetime.now().strftime('%Y-%m-%d %H:%M:')  # Format: YYYY-mm-dd hh:mm:
    random_suffix = f"{random.randint(0, 60):02}"  # Generate a random number between 0 and 60, formatted as two digits
    current_time_standard = current_time_standard + random_suffix  # Append the random number to the current_time string
    current_time_iso = current_time_standard.replace(' ', 'T')  # Convert format to YYYY-mm-ddThh:mm:ss
    current_time=current_time
    operation = config.get('Basic_info', 'Operation')  # Reads Operation from the ini file
    key_Part_Number = 'UNKNOWPN'
    xml_filename = (  # Constructs the XML filename
        f"Site={site},ProductFamily={product_family},Operation={operation},"
        f"Partnumber={key_Part_Number},"
        f"Serialnumber={current_time},"
        f"Testdate={current_time_iso}.xml"
    ).replace(':', '.').replace('/', '-').replace('\\', '-')
    xml_filepath = os.path.join(output_path, xml_filename)  # Constructs the full path for the XML file

    with open(xml_filepath, 'w', encoding='utf-8') as f:  # Opens the XML file in write mode
        f.write('<?xml version="1.0" encoding="utf-8"?>\n')  # Writes the XML declaration
        f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')  # Writes the root element start tag
        f.write(f'    <Result startDateTime="{current_time_iso}" endDateTime="{current_time_iso}" Result="Passed">\n')  # Writes the Result element and its attributes
        f.write(f'       <Header SerialNumber="{current_time}" PartNumber="{key_Part_Number}" Operation="{operation}" TestStation="NA" Operator="NA" StartTime="{current_time_iso}" Site="{site}" LotNumber="" Quantity=""/>\n')  
        f.write('        <HeaderMisc>\n')  
        f.write('              <Item Description=""/>\n')  
        f.write('        </HeaderMisc>\n') 
        f.write(f'        <TestStep Name="{operation}" startDateTime="{current_time_iso}" endDateTime="{current_time_iso}" Status="Passed">\n') 
        f.write(f'                <Data DataType="Table" Name="tbl_{operation.upper()}" Value="{csv_output_path}" CompOperation="LOG"/>\n')  # Writes the Data element, pointing to the CSV file
        f.write('        </TestStep>\n')  # Writes the TestStep element end tag
        f.write('    </Result>\n')  # Writes the Result element end tag
        f.write('</Results>\n')  # Writes the root element end tag
    Log.Log_Info(global_log_file, f"XML File Created: {xml_filepath}")  # Logs the successful creation of the XML file

def process_ini_file(config_path: str) -> None:  # Defines the process_ini_file function to handle .ini configuration files
    """Reads the specified .ini file and performs Excel and XML processing."""  # Function description: Executes relevant processing based on the configuration file
    global global_log_file  # Uses the global variable global_log_file
    config = ConfigParser()  # Creates a ConfigParser object to parse the configuration file
    try:  # Tries to read the configuration file
        with open(config_path, 'r', encoding='utf-8') as config_file:  # Opens the .ini file in read mode
            config.read_file(line for line in config_file if not line.strip().startswith('#'))  # Reads the file, skipping comment lines starting with '#'
    except Exception as e:  # Catches all exceptions
        Log.Log_Error(global_log_file, f"Error reading config file {config_path}: {e}")  # Logs an error while reading the config file
        return  # Exits the function

    try:  # Tries to get various configurations from the config file
        input_paths = [path.strip() for path in config.get('Paths', 'input_paths').splitlines() if path.strip() and not path.strip().startswith('#')]  # Gets the list of input paths, filtering out empty and comment lines
        output_path = config.get('Paths', 'output_path')  # Gets the output path
        running_rec = config.get('Paths', 'running_rec')  # Gets the running record file path
        sheet_name = config.get('Excel', 'sheet_name')  # Gets the Excel sheet name
        data_columns = config.get('Excel', 'data_columns')  # Gets the data columns to be read
        log_path = config.get('Logging', 'log_path')  # Gets the log storage path
        fields_config = [field.strip() for field in config.get('DataFields', 'fields').splitlines() if field.strip()]  # Gets the data field configuration, filtering out empty lines
        site = config.get('Basic_info', 'Site')  # Gets the site information
        product_family = config.get('Basic_info', 'ProductFamily')  # Gets the product family information
        operation = config.get('Basic_info', 'Operation')  # Gets the operation name
        Test_Station = config.get('Basic_info', 'TestStation')  # Gets the test station information
        file_name_pattern = config.get('Basic_info', 'file_name_pattern')  # Gets the file name matching pattern

    except NoSectionError as e:  # If a section is missing in the configuration
        Log.Log_Error(global_log_file, f"Missing section in config file {config_path}: {e}")  # Logs an error
        return  # Exits the function
    except NoOptionError as e:  # If an option is missing in the configuration
        Log.Log_Error(global_log_file, f"Missing option in config file {config_path}: {e}")  # Logs an error
        return  # Exits the function

    log_folder_name = str(datetime.today().date())  # Uses today's date as the log folder name
    log_folder_path = os.path.join(log_path, log_folder_name)  # Constructs the log folder path
    if not os.path.exists(log_folder_path):  # If the folder does not exist
        os.makedirs(log_folder_path)  # Creates the log folder
    log_file = os.path.join(log_folder_path, '043_LD-SPUT.log')  # Constructs the full log file path
    global_log_file = log_file  # Updates the global variable global_log_file
    setup_logging(global_log_file)  # Calls setup_logging to configure logging
    Log.Log_Info(log_file, f"Program Start for config {config_path}")  # Logs the program start message

    fields = {}  # Initializes the field configuration dictionary
    for field in fields_config:  # Iterates through each line of the field configuration
        if field.strip():  # If the line is not empty
            key, col, dtype = field.split(':')  # Splits the line to get the key, column number, and data type
            fields[key.strip()] = (col.strip(), dtype.strip())  # Stores the configuration in the dictionary

    for input_path in input_paths:  # Iterates through all input paths
        print(input_path)  # Prints the currently processed input path,
        files = glob.glob(os.path.join(input_path, file_name_pattern))  # Gets the file list based on the matching pattern
        files = [file for file in files if not os.path.basename(file).startswith('~$')]  # Filters out temporary files
        if not files:  # If no files are found
            Log.Log_Error(global_log_file, f"Can't find Excel file in {input_path} with pattern {file_name_pattern}")  # Logs an error
        for file in files:  # Iterates through each matched file
            if not os.path.basename(file).startswith('~$'):  # If the filename does not start with '~$'
                destination_dir = config.get('Paths', 'copy_destination_path')  # Gets the destination directory from the [Paths] section of the ini file
                if not os.path.exists(destination_dir):  # If the destination directory does not exist
                    os.makedirs(destination_dir)  # Creates the destination directory
                shutil.copy(file, destination_dir)  # Copies the file to the destination directory
                Log.Log_Info(global_log_file, f"Copy excel file {file} to ../DataFile/047_TAK_SPC/")  # Logs the file copy message
                copied_file_path = os.path.join(destination_dir, os.path.basename(file))  # Constructs the full path of the copied file
                process_excel_file(copied_file_path, sheet_name, data_columns, running_rec,
                                   output_path, fields, site, product_family, operation, Test_Station, config)  # Processes the Excel file

def main() -> None:  # Defines the main function
    """Scans all .ini files and executes processing."""  # Function description: Iterates through all .ini files in the current directory and processes them according to the configuration
    ini_files = glob.glob("*.ini")  # Gets a list of all .ini files in the current directory
    for ini_file in ini_files:  # Iterates through each .ini file
        process_ini_file(ini_file)  # Processes the .ini file

if __name__ == '__main__':  # If this module is run as the main program
    main()  # Calls the main function
    Log.Log_Info(global_log_file, "Program End")  # Logs the program end message