import configparser
import os
import glob
import pandas as pd

def read_ini_files(directory):
    config = configparser.ConfigParser()
    ini_files = glob.glob(os.path.join(directory, '*.ini'))
    settings = {}
    
    for ini_file in ini_files:
        with open(ini_file, 'r', encoding='utf-8', errors='ignore') as f:
            config.read_file(f)
        for section in config.sections():
            settings[section] = {}
            for key, value in config.items(section):
                settings[section][key] = value
    
    return settings

def read_csv_files(input_paths, file_name_pattern):
    csv_files = glob.glob(os.path.join(input_paths, file_name_pattern))
    dataframes = []
        
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        last_modified_time = os.path.getmtime(csv_file)
        last_modified_time_str = pd.to_datetime(last_modified_time, unit='s').strftime('%Y-%m-%dT%H:%M:%S')
        df['last_modified_time'] = last_modified_time_str
        dataframes.append(df)
        
    return dataframes

def process_dataframes(dataframes):
    processed_dataframes = []
        
    for df in dataframes:
            # Example processing: remove rows with missing values
        data_fields = {
            'date_time': str,
            'scriber': float,
            'cleaving': float,
            'Lot': str,
            'operator': str,
            'Needle_maker': str,
            'Needle_No': float,
            'scribe_length': float,
            'scribe_force': float,
            'unseparate': float,
            'peeling': float
        }
        df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['time']).dt.strftime('%Y/%m/%d %H:%M:%S')
        df = df[['date_time'] + [col for col in df.columns if col not in ['Date', 'time', 'date_time']]]
        df = df.dropna()        
        cols = ['date_time'] + [col for col in df.columns if col != 'date_time']
        df = df[cols]
        one_month_ago = pd.Timestamp.now() - pd.DateOffset(months=1)
        df = df[df['date_time'] >= one_month_ago.strftime('%Y/%m/%d %H:%M:%S')]
        df['scriber'] = df['scriber'].str.extract(r'\.(\d+)', expand=False)
        df['cleaving'] = df['cleaving'].str.extract(r'\.(\d+)', expand=False)
        for column, target_type in data_fields.items():
            if column in df.columns:
                try:
                    if target_type == float:
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif target_type == str:
                        df[column] = df[column].astype(str)
                    elif target_type == int:
                        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                    elif target_type == pd.Timestamp:
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                except Exception as e:
                    print(f"Error converting column {column} to {target_type}: {e}")
        
        processed_dataframes.append(df)
        # Remove rows where specific columns do not match the required data types
        for column in ['scriber', 'cleaving', 'Needle_No', 'scribe_length', 'scribe_force', 'unseparate', 'peeling']:
            if column in df.columns:                
                df = df[pd.to_numeric(df[column], errors='coerce').notnull()]

    return processed_dataframes

def validate_dataframes(dataframes, data_fields):
    validated_dataframes = []
        
    for df in dataframes:
        valid_rows = []
        for _, row in df.iterrows():
            valid = True
            for field, dtype in data_fields.items():
                if field in row and not isinstance(row[field], dtype):
                    valid = False
                    break
            if valid:
                valid_rows.append(row)
        validated_df = pd.DataFrame(valid_rows, columns=df.columns)
        validated_dataframes.append(validated_df)
        
        return validated_dataframes


def generate_xml(data_dict,output_file_name):   
        print(data_dict.get('key_Start_Date_Time', ''))
        directory = os.getcwd()
        settings = read_ini_files(directory)
        output_file_name = settings['Paths']['output_path'] + output_file_name
        xml_filename = f"Site={data_dict['key_Site']},ProductFamily={data_dict['ProductFamily']},Operation={data_dict['key_Operation']},PartNumber={data_dict.get('key_Part_Number', 'Unknown')},SerialNumber={data_dict.get('key_Serial_Number', 'Unknown')},Testdate ={data_dict.get('key_Start_Date_Time','Unkonow')}.xml"
        xml_filepath = os.path.join(settings['Paths']['xml_path'], xml_filename)
        with open(xml_filepath, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="utf-8"?>\n')
            f.write('<Results xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n')
            f.write(f'    <Result startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" endDateTime="{data_dict["key_End_Date_Time"].replace(".", ":")}" Result="Passed">\n')
            f.write(f'        <Header SerialNumber="{data_dict["key_Serial_Number"]}" PartNumber="{data_dict["key_Part_Number"]}" Operation="{data_dict["key_Operation"]}" TestStation="{data_dict["key_Test_Station"]}" Operator="{data_dict["key_Operator"]}" StartTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" Site="{data_dict['key_Site']}" LotNumber="" Quantity="" />\n')
            f.write('        <HeaderMisc>\n')
            f.write('            <Item Description="BE_Scriber_Cleaving_Monitor"></Item>\n')
            f.write('        </HeaderMisc>\n')
            f.write(f'        <TestStep Name="{data_dict["key_Operation"]}" startDateTime="{data_dict["key_Start_Date_Time"].replace(".", ":")}" endDateTime="{data_dict["key_End_Date_Time"].replace(".", ":")}" Status="Passed">\n')
            f.write(f'            <Data DataType="Table" Name="{data_dict['key_Operation']}" Units="" Value="{output_file_name}" CompOperation="" />\n')
            f.write('        </TestStep>\n')
            f.write('    </Result>\n')
            f.write('</Results>\n')


def main():
    directory = os.getcwd()
    settings = read_ini_files(directory)
    input_paths = settings['Paths']['input_paths']
    file_name_pattern = settings['Basic_info']['file_name_pattern'] 
    # Example usage
    dataframes = read_csv_files(input_paths, file_name_pattern)
    dataframes = process_dataframes(dataframes)
    output_path = settings['Paths']['output_path']
    operation = settings['Basic_info']['operation']
    for df in dataframes:
        if 'last_modified_time' in df.columns:
            first_last_modified_time = df['last_modified_time'].iloc[0]
            df = df.drop(columns=['last_modified_time'])
            output_file_name = f"{operation}_{pd.to_datetime(first_last_modified_time).strftime('%Y%m%d%H%M%S')}.csv"
            df.to_csv(os.path.join(directory, output_path, output_file_name), index=False)

    data_dict ={}
    data_dict = {
        'key_Start_Date_Time': pd.to_datetime(min(df['date_time'])).strftime('%Y-%m-%dT%H.%M.%S'),
        'key_End_Date_Time': pd.to_datetime(max(df['date_time'])).strftime('%Y-%m-%dT%H:%M:%S'),
        'key_Serial_Number': pd.to_datetime(first_last_modified_time).strftime('%Y%m%d%H%M%S'),
        'lot_number': df['Lot'].iloc[0],
        'key_Site': settings['Basic_info']['site'],
        'ProductFamily': settings['Basic_info']['productfamily'],
        'key_Part_Number': "UNKNOWNPN",
        'key_Operation': settings['Basic_info']['operation'],
        'key_Test_Station': "NA",
        'key_Operator': df['operator'].iloc[0],
        'key_DataType': "Table",
        'key_Name': "SAG_BE_Scriber_Cleaving_Monitor",
        'XML_PATH' : settings['Paths']['xml_path']
    }
    generate_xml(data_dict,output_file_name)    
    
if __name__ == "__main__":
    main()