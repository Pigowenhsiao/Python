
# 指数表記(N)を展開して返す
def Expand(N):

    # float型で送られてくるので、文字列に変更 -> 年のため、eを小文字変換しておく
    N = str(N).lower()


    # 指数表記が+か-かで分岐
    # 指数が負のとき
    if N.count('-') == 2 or (N[0] != '-' and N.count('-') == 1):

        # 先頭に符号がある場合は事前にとっておき、フラグを立てておく
        if N[0] == '-':
            N = N[1:]
            Flag = 1
        else:
            Flag = 0

        S = ""
        # eの場所
        E_Index = N.index('e')

        # 小数点位置の取得
        if '.' in N:
            Decimal_point_Index = N.index('.')
        else:
            Decimal_point_Index = E_Index

        # 指数の取得
        Index_Num = int(N[E_Index + 2:])

        # 指数の展開
        if Decimal_point_Index > Index_Num:
            # "0.xxxxx..."の形ではない
            S += N[:Decimal_point_Index - Index_Num] + '.' + N[Decimal_point_Index - Index_Num:E_Index].replace('.', '')
        else:
            # "0.xxxxx..."の形
            S += '0.' + '0' * (Index_Num - Decimal_point_Index) + N[:E_Index].replace('.', '').rstrip('0')

        if Flag:
            S = '-'+S

        return S


    # 指数が正のとき
    else:
        return int(float(N))
