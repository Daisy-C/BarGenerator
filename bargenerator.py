import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pandas import Series, DataFrame
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

''' pandas数据打印显示设置 '''
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
# 设置一行的显示宽度
pd.set_option('display.width', 1000)

''' 全局变量 '''
# 读取数据 dt ——> data
dt = pd.read_parquet('data.parquet', engine='pyarrow')
# 丢弃数据集中任何含有缺失值的行
dt = dt.dropna()
# 所有合约的ID
InsID = dt['InstrumentID'].unique()
# 所有交易所的ID
ExID = dt['ExchangeID'].unique()
# 所有交易所的名称
ExName = {'DCE': '大连商品交易所', 'SHFE': '上海期货交易所', 'CZCE': '郑州商品交易所', 'INE': '上海国际能源交易中心'}


# 数据检查
def data_check():
    print("========================================数据检查结果========================================")
    print("=======================================总体数据检查结果======================================")
    # 统计LocalTime单调性
    local_time = dt['LocalTime']
    monotony = [local_time[i] <= local_time[i + 1] for i in range(len(local_time) - 1)]
    if False in monotony:
        if True in monotony:
            print(" · LocalTime数据不具有单调性")
        else:
            print(" · LocalTime数据单调递减")
    else:
        print(" · LocalTime数据单调递增")
    # 探索LastPrice是否满足UpperLimitPrice和LowerLimitPrice的限制
    flag = 1
    for i in range(len(dt)):
        if dt.iloc[i]['UpperLimitPrice'] >= dt.iloc[i]['LastPrice'] >= dt.iloc[i]['LowerLimitPrice']:
            continue
        else:
            flag = 0
            print(" · 交易所{}合约{}的LastPrice不满足范围限制".format(dt.iloc[i]['ExchangeID'], dt.iloc[i]['InstrumentID']))
    if flag:
        print(" · 所有数据中LastPrice都满足UpperLimitPrice和LowerLimitPrice的限制")
    print('')

    # 分交易所数据检查
    print("=====================================分交易所数据检查结果====================================")
    for exchange in ExID:
        print("{}-{}：".format(exchange, ExName[exchange]))
        # 提取出此交易所推送的数据
        view_certain_exchange = dt[dt['ExchangeID'] == exchange]
        # 分交易所统计(UpdateTime, UpdateMyllisec)单调性
        ordered = view_certain_exchange.copy()
        ordered = ordered.sort_values(by=['UpdateTime', 'UpdateMillisec'])
        flag = 1
        for i in range(len(view_certain_exchange)):
            if (view_certain_exchange.iloc[i] != ordered.iloc[i]).any():
                flag = 0
                break
        if flag:
            print(" · (UpdateTime, UpdateMyllisec)单调递增")
        else:
            print(" · (UpdateTime, UpdateMyllisec)不单调")

        # 分交易所探索ActionDay，TradingDay和实际交易时间的关系 画出关系图
        action_day = view_certain_exchange["ActionDay"].unique()
        trading_day = view_certain_exchange["TradingDay"].unique()
        print(" · ActionDay 的范围:{}".format(action_day))
        print(" · TradingDay的范围:{}".format(action_day))
        ''' 关系图绘制 '''
        fig, ax = plt.subplots()
        ax.plot(view_certain_exchange['UpdateTime'], view_certain_exchange['ActionDay'], label="ActionDay")
        ax.plot(view_certain_exchange['UpdateTime'], view_certain_exchange['TradingDay'], label="TradingDay")
        ax.xaxis.set_major_locator(ticker.MultipleLocator(5000))
        plt.xlabel("UpdateTime")
        plt.ylabel("Day")
        plt.title(exchange + ": Relation of TradingDay & ActionDay")
        plt.legend()
        plt.savefig('./TARelation/'+exchange+' TARelation.png', dpi=800)
        plt.show()
        ''' 找出分界点'''
        flag = 1
        for i in range(len(view_certain_exchange) - 1):
            if view_certain_exchange.iloc[i]['ActionDay'] != view_certain_exchange.iloc[i]['TradingDay'] and \
                    view_certain_exchange.iloc[i + 1]['ActionDay'] == view_certain_exchange.iloc[i + 1]['TradingDay']:
                print(" · ActionDay,TradingDay {}及之前不一致，之后一致".format(view_certain_exchange.iloc[i]['UpdateTime']))
                flag = 0
                break
        if flag:
            print(" · ActionDay,TradingDay一致")

        # 分交易所探索tick推送频率
        num = len(view_certain_exchange)
        time = view_certain_exchange.iloc[num - 1]["LocalTime"] - view_certain_exchange.iloc[0]["LocalTime"]
        time = time // 1000000000  # 纳秒时间转换为秒
        freq = num / time
        print(" · 推送交易所下所有合约的频率为{}次/秒".format(freq))

        # 检查完一个交易所的数据后 换行
        print('')

    # 分合约数据检查
    print("=====================================分合约数据检查结果======================================")
    Non_Mono_Volume = []
    Non_Mono_Turnover = []
    up_not_fix = set()
    low_not_fix = set()
    for instrument in InsID:
        # 提取出此合约的数据
        certain_instrument = dt[dt['InstrumentID'] == instrument]
        # 统计成交量Volume，成交额Turnover的单调性
        volume = certain_instrument['Volume']
        turnover = certain_instrument['Turnover']
        v_monotony = [volume.iloc[i] <= volume.iloc[i + 1] for i in range(len(volume) - 1)]
        t_monotony = [turnover.iloc[i] <= turnover.iloc[i + 1] for i in range(len(turnover) - 1)]
        if False in v_monotony:
            Non_Mono_Volume.append(instrument)
        if False in t_monotony:
            Non_Mono_Turnover.append(instrument)

        # 统计UpperLimitPrice, LowerLimitPrice值是否固定
        up = certain_instrument['UpperLimitPrice'].unique()
        low = certain_instrument['LowerLimitPrice'].unique()
        if len(up) > 1:
            up_not_fix.add(instrument)
        if len(low) > 1:
            low_not_fix.add(instrument)

        # 探索合约在各交易所的交易情况 / 探索合约委托在哪些交易所
        notone = 0
        exs = certain_instrument["ExchangeID"].unique()
        if len(exs) > 1:
            notone = 1
            print("合约{}委托在交易所{}".format(instrument, exs))

    # 给出成交量Volume，成交额Turnover单调性的结论
    if Non_Mono_Volume == list():
        print(" · 所有合约的Volume数据单调递增")
    else:
        print(" · 以下合约的Volume数据不满足单调递增：{}".format(Non_Mono_Volume))
    if Non_Mono_Turnover == list():
        print(" · 所有合约的Turnover数据单调递增")
    else:
        print(" · 以下合约的Turnover数据不满足单调递增：{}".format(Non_Mono_Turnover))

    # 给出UpperLimitPrice, LowerLimitPrice值是否固定的结论
    flag = 1
    if up_not_fix != set():
        flag = 0
        print(" · 以下合约UpperLimitPrice值不固定：{}".format(up_not_fix))
    if low_not_fix != set():
        flag = 0
        print(" · 以下合约LowerLimitPrice值不固定：{}".format(low_not_fix))
    if flag == 1:
        print(" · 所有合约的UpperLimitPrice值、LowerLimitPrice值均固定")
    # 给出合约委托交易所数的结论
    if notone == 0:
        print(" · 所有合约都只委托给一家交易所")
    print("")

    print("=======================================分交易所、合约=======================================")
    for exchange in ExID:
        print("{}-{}：".format(exchange, ExName[exchange]))
        exc = dt[dt["ExchangeID"] == exchange]
        instruments = exc["InstrumentID"].unique()
        freq_list = []
        min_time_diff = 0
        for instrument in instruments:
            multiples = set()
            ins = exc[exc["InstrumentID"] == instrument]
            # 分 (交易所,合约) 探索Volume，Turnover，LastPrice的关系
            for i in range(len(ins)):
                if i == 0:
                    continue
                elif i == 1:
                    min_time_diff = ins.iloc[i]['LocalTime'] - ins.iloc[i-1]['LocalTime']
                elif ins.iloc[i]['LocalTime'] - ins.iloc[i-1]['LocalTime'] < min_time_diff:
                    min_time_diff = ins.iloc[i]['LocalTime'] - ins.iloc[i-1]['LocalTime']

                if ins.iloc[i]['Volume'] != ins.iloc[i - 1]['Volume']:
                    diff_volume = ins.iloc[i]['Volume'] - ins.iloc[i - 1]['Volume']
                    diff_turnover = ins.iloc[i]['Turnover'] - ins.iloc[i - 1]['Turnover']
                    price = diff_turnover / diff_volume
                    multiple = round(price / ins.iloc[i]['LastPrice'])
                    if multiple > 100:
                        multiple = round(multiple, -1)
                    multiples.add(multiple)
            if multiples == set():
                print(" · 合约{}: 没有成交记录".format(instrument))
            else:
                print(" · 合约{}: 每次成交期货成交量可为{}".format(instrument, multiples))
            # 计算各个合约的平均推送频率
            num = len(ins)
            time = ins.iloc[num - 1]["LocalTime"] - ins.iloc[0]["LocalTime"]
            time = time // 1000000000  # 纳秒时间转换为秒
            freq = num / time
            freq_list.append(freq)
            # 两个数据同时结果输出
            # if multiples == set():
            #     print(" · 合约{}: 没有成交记录,推送频率为{}".format(instrument, round(freq, 2)))
            # else:
            #     print(" · 合约{}: 每次成交期货成交量可为{},总体平均推送频率为{}".format(instrument, multiples, round(freq, 2)))
        print("最大平均tick推送频率：{}次/秒".format(round(np.max(freq_list), 2)))
        print("最小平均tick推送频率：{}次/秒".format(round(np.min(freq_list), 2)))
        print("峰值tick推送频率：{}次/秒".format(round(1/(min_time_diff // 1000000000), 2)))
        print("")  # 换行

    print("========================================数据检查结束========================================")
    return


# 合成分钟bar
def minute_bar():
    # 将LocalTime转换到中国时区时间 年-月-日 时:分:秒 便于按分钟处理数据
    dt['LocalTime'] = [datetime.fromtimestamp(x // 1000000000).strftime("%Y-%m-%d %H:%M:%S") for x in dt['LocalTime']]

    # 用于保存bar数据,字典类型，每个合约对应一个dataframe
    result = {}
    # 用于暂时记录数据
    record = {}
    LastPrice = {}
    PreVolume = {}
    time_record = []
    for instrument in InsID:
        result[instrument] = pd.DataFrame(columns=["LocalTime", 'Opening', 'Max', 'Min', 'Closing'])
        record[instrument] = list()
        ins_data = dt[dt["InstrumentID"] == instrument]
        LastPrice[instrument] = ins_data.iloc[0]['LastPrice']
        PreVolume[instrument] = ins_data.iloc[0]['Volume']

    ''' 模拟流式行情接收 按照LocalTime顺序一条条读数据 合成每分钟每个合约的bar'''
    cnt = len(dt)
    for i in range(cnt):
        localtime = dt.iloc[i]["LocalTime"]
        localtime = localtime[:-2]+'00'    # 秒位归零，使得同一分钟数据时间戳相同
        # 一分钟结束 即出现新的分钟时间戳，或读到最后一条数据
        if not (localtime in time_record) or i == (cnt-1):
            # 记录时间
            time_record.append(localtime)
            # 第一条数据跳过
            if i == 0:
                continue
            ''' 一分钟结束 计算bar、清空数据 '''
            for instrument in InsID:
                # 计算bar
                if record[instrument] == list():
                    opening = LastPrice[instrument]
                    max_p = 0
                    min_p = 0
                    closing = LastPrice[instrument]
                else:
                    opening = record[instrument][0]
                    max_p = max(record[instrument])
                    min_p = min(record[instrument])
                    closing = record[instrument][-1]
                # 插入行数据
                new = pd.DataFrame({"LocalTime": [time_record[-1]], 'Opening': [opening], 'Max': [max_p], 'Min': [min_p], 'Closing': [closing]})
                result[instrument] = result[instrument].append(new)
                # 处理完后，清空暂存的合约tick数据
                record[instrument] = list()
        # 获取合约ID
        ins = dt.iloc[i]['InstrumentID']
        # 成交记录，记录价格数据
        if dt.iloc[i]['Volume']-PreVolume[ins] > 0:
            record[ins].append(dt.loc[i]['LastPrice'])
        # 更新合约的数据，存入字典
        LastPrice[ins] = dt.iloc[i]['LastPrice']
        PreVolume[ins] = dt.iloc[i]['Volume']
    ''' 补充检查每个合约得到的分钟bar数目是否正确 '''
    # for instrument in InsID:
    #     print("合约{}的结果 有条{}数据".format(instrument, len(result[instrument])))
    #     print("Record中剩余{}条数据没有处理".format(len(record[instrument])))

    # 创建新的openpyxl工作簿
    book = Workbook()
    # 存储数据，每个合约对应一个sheet
    for instrument in InsID:
        sheet = book.create_sheet(instrument)
        for row in dataframe_to_rows(result[instrument], index=False, header=True):
            sheet.append(row)
    # 删除多余的默认sheet
    del book["Sheet"]
    book.save("Bar.xlsx")
    return


if __name__ == '__main__':
    data_check()
    minute_bar()