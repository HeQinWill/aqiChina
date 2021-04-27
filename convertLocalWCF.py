from io import StringIO
from wcf.records import Record, print_records
from contextlib import redirect_stdout
import xml.dom.minidom
import pandas as pd
import numpy as np
from pathlib import Path

def xmlparse(xmlstr):
    '''
    parse air quality xml data to dict list
    '''
    dom = xml.dom.minidom.parseString(xmlstr)
    root = dom.documentElement
    stats = dom.getElementsByTagName("AQIDataPublishLive")
    airdata = []
    for stat in stats:
        # print len(stat.childNodes)
        r = {}
        for node in stat.childNodes:
            if (node.nodeName == "#text" or
                    node.nodeName == "OpenAccessGenerated"):
                continue
            inx = node.nodeName
            inx = inx.lower()
            for n in node.childNodes:
                # print n.data
                r[inx] = n.data
        airdata.append(r)
    return airdata
    
def data_from_xml_json(xmlfile):
    '''
    预处理由 wcf 转换后的 xml，将不必要的字符删除
    '''
    # fp = open(xmlfile) # 如果是之前存储下来的文件需要先 open
    # data = fp.read()
    data = xmlfile

    # this is for the air quality data, to split some charicters
    data = data.replace("a:", "")
    data = data.replace("b:", "")
    data = data.replace("c:", "")
    data = data.replace("&mdash", "-")
    return xmlparse(data)

# 尽量用后一个时刻的结果（也就是第 13 分钟），可能数据更全些 
fileList = list(Path('.').glob('cnemc_*13'))
# fileList = list(Path('.').glob('cnemc_*43'))

for f in fileList[:]:
    print(f)
    r = open(f,'rb')
    records = Record.parse(r)

    # 将print在std.out的内容赋予变量out_xml
    f = StringIO()
    with redirect_stdout(f):
        print_records(records)
    out_xml = f.getvalue()
    
    # 将数据从总的 xml 转为各个 dict 组成的 list
    data_dict = data_from_xml_json(out_xml)
    
    # 将字典转为 DataFrame
    df = pd.DataFrame.from_dict(data_dict)
    # 如果出现不同的时间就把全部数据输出一份
    if len(df['timepoint'].unique())>1:
        df.to_csv('Problem'+df['timepoint'].unique()[0][:13]+'.csv',index=None)

    # 将数据处理下再输出
    df_ = df[['stationcode','timepoint','area', 'positionname', 'latitude', 'longitude',
            'aqi','pm10', 'pm10_24h', 'pm2_5', 'pm2_5_24h','o3', 'o3_24h', 'o3_8h', 'o3_8h_24h',
            'no2', 'no2_24h', 'so2', 'so2_24h','co', 'co_24h','primarypollutant']]
    df_ = df_.where(df_!='-;',np.nan) 
    # 将每小时数据保存为 csv 文件
    df_.to_csv(df['timepoint'].unique()[-1][:13]+'.csv',index=None)
