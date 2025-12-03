
# テキストファイルから開始する行数を取得
def start_row_number(path):
    textfile = open(path,"r",encoding="utf-8")
    row_number = int(textfile.readline())
    textfile.close()
    return row_number

# テキストファイルに次以降の開始位置を上書きで書き込む
def next_start_row_number(path, row_number):
    textfile = open(path, "w", encoding="utf-8")
    textfile.write(str(row_number))
    textfile.close()