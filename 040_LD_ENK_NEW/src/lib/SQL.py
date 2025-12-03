import pyodbc

# SQLサーバへの接続
def connSQL():
    driver = '{SQL Server}'
    server = '192.168.117.140'
    database = 'PrimeProd'
    username = 'prime-mfg'
    password = 'manufacturing'
    try:
        conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = conn.cursor()
        print("Connection:OK")
        return conn, cursor
    except pyodbc.Error as err:
        # Noneを返し、呼び出し元でエラー処理を行う。
        print("Connection:NG")
        return None, None

# ロット番号をキーとした品名検索と併せて9桁ロット番号も返す
def selectSQL(cursor, serial_number):
    serial_number = str(serial_number)
    part_number = None
    nine_serial_number = None
    cursor.execute("select ProductName, ContainerName from prime.v_LotStatus where ContainerName Like '____" + serial_number + "';")
    row = cursor.fetchone()
    if row is not None and len(row[1])==9:
        part_number = row[0]
        nine_serial_number = row[1]
    print(serial_number, part_number, nine_serial_number)
    return part_number, nine_serial_number

# SQLサーバから切断
def disconnSQL(conn, cursor):
    cursor.close()
    conn.close()
    print("Disconnection:OK")