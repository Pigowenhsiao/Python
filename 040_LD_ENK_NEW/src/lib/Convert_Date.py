# date書式変更
from datetime import datetime, timedelta

# 日付の形式統一
def Edit_Date(start_date_time):
    print("start_date_time: ", start_date_time)
    ZENKAKU = ['１','２','３','４','５','６','７','８','９']
    for s in str(start_date_time):
        if s in ZENKAKU:
            print("ZENKAKU error")
            return "ZENKAKU error"
    try:
        # 形式が6/15/2020"の場合、float型になるのでDate型に直す
        if type(start_date_time) is float:
            edit_start_date_time = (datetime(1899, 12, 30) + timedelta(days=start_date_time)).strftime('%Y-%m-%dT%H.%M.%S')
            print("edit_start_date_time: ", edit_start_date_time)

        # 末尾に"E"がついているとstr型になるのでDate型に直す
        elif type(start_date_time) is str:
            start_date_time = start_date_time.replace("T", "/")
            start_date_time = start_date_time.replace(".", "/")
            start_date_time = start_date_time.replace("E", "")
            start_date_time = start_date_time.replace("e", "")
            start_date_time = start_date_time.replace("～", "")
            start_date_time = start_date_time.replace("-", "/")
            
            split_date = start_date_time.split('/')
            
            # YearとMonthの間の"/"が抜けていることがあるので除外する
            if len(split_date) < 3:
                edit_start_date_time = ""
            else:
                year = split_date[0]
                month = split_date[1]
                day = split_date[2]

                # 日付判定
                if len(month)!=0 and len(day)!=0 and 1<=int(month)<=12 and 1<=int(day)<=31:

                    # 一桁の場合に先頭に0を付与すること、dayの末尾に"E"が入っていたら撤廃すること
                    if len(month) == 1: month = '0' + month
                    if day[-1] == 'E': day = day[:-1]
                    if len(day) == 1: day = '0' + day

                    edit_start_date_time = year + '-' + month + '-' + day + 'T00.00.00'
                    print("edit_start_date_time: ", edit_start_date_time)
                else:
                    edit_start_date_time = ""
                    print("日付判定 error")
        else:
            edit_start_date_time = str(start_date_time).replace(" ", "T").replace(":", ".")

        # 年数を2桁で入れていることがあるので、その場合は除外する
        if len(edit_start_date_time) != 19:
            #edit_start_date_time = ""
            edit_start_date_time = edit_start_date_time[:19]
            print("edit_start_date_time", edit_start_date_time)

        return edit_start_date_time

    except Exception as e:
        return f"Error: {str(e)}"