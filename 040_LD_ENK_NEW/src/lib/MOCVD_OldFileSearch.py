
"""

Epi_Numberは、各着工ファイルの番号のみを取得している。
基本的には[XX0001~.xlsx]となっており、0001を取得しているが、
[XX0001~XX0100.xlsx]とファイルもあり、その場合00010100を取得してしまう。

全て直すのが手間だったため、こちらで4文字のみの取得にとどめる。
[00010100]であれば先頭の0001だけを取得し、[0001]が入ったExcelファイルのうち作成日時が新しいものを返す。
なければ-1を返し、呼び出し元でエラーを返して終了させる


"""

import os
from time import localtime, strftime

def F1(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F1炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]


def F2(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F2炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]


def F3(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F3炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]


def F4(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F4炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]


def F5(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F5炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            print(f)
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]


def F6(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F6炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]


def F7(Epi_Number):

    Epi_Number = Epi_Number[:4]

    # Pathの定義
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F7炉/'

    # フォルダ下の全ファイルを取得し、ファイル名に与えられたEpi_Numberが含まれているか確認する

    # Epi_Numberが含まれていたら、そのファイルパスを格納する(ファイルパスとファイル作成日時のセット)
    ret_List = []

    # Path下の全ファイルを探索
    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '~$' not in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                ret_List.append([filepath, dt])

    # ret_Listが空であれば-1を返す
    if len(ret_List) == 0:
        return -1

    # 作成日時順に並び替え、先頭にあるパスを返す
    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]

def F9(Epi_Number):
    Epi_Number = Epi_Number[:4]
    Path = 'Z:/MOCVD/MOCVD過去プログラム/F9炉/'
    ret_List = []

    for path, dir, file in os.walk(Path):
        for f in file:
            if (f.endswith('.xlsx') or f.endswith('.xlsm') or f.endswith('.xls')) and Epi_Number in str(f) and '$' not in str(f):
                filepath = os.path.join(path, f)
                print(filepath)
                dt = strftime("%Y-%m-%d %H:%M:%S", localtime(os.path.getctime(filepath)))
                print(dt)
                ret_List.append([filepath, dt])
                input('enter')

    if len(ret_List) == 0:
        return -1

    ret_List = sorted(ret_List, key=lambda x: x[1], reverse=True)
    return ret_List[0][0]
