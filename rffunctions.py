import numpy as np
import pandas as pd
import seaborn
import matplotlib.pyplot as plt
import sklearn
import graphviz
import itertools
from tqdm import tqdm
from datetime import datetime
from datetime import timedelta



def merging(netfile, physfile, name):
    dfnet = pd.read_csv(netfile)
    dfphys = pd.read_csv(physfile)

    format1 = '%d/%m/%Y %H:%M:%S'
    format2 = '%Y-%d-%m %H:%M:%S'

    dfphys['Time'] = pd.to_datetime(dfphys['Time'], format = format1).dt.strftime(format2)

    d = {}
    for i, r in tqdm(dfphys.iterrows(), total=(len(dfphys))):
        upper = datetime.strptime(r['Time'], '%Y-%d-%m %H:%M:%S')
        lower = upper - timedelta(seconds=1)
        timeupper = str(upper)
        timelower = str(lower)
        dftemp = dfnet[(dfnet['Time'] < timeupper) & (dfnet['Time'] > timelower)]
        if dftemp.shape[0] ==0:
            dftemp = dfnet[(dfnet['Time'] > timeupper)]
            choice = dftemp.iloc[0]
        else:
            choice = dftemp.iloc[-1]
        d[i] = choice

    Time = []
    mac_s = []
    mac_d = []
    ip_s = []
    ip_d = []
    sport = []
    dport = []
    proto = []
    flags = []
    size = []
    modbus_fn = []
    n_pkt_src = []
    n_pkt_dst = []
    modbus_response = []
    label_n = []
    label = []

    litemp = []

    for dkey in d.keys():
        for skey in d[dkey].keys():
            litemp.append(d[dkey][skey])

    li = [Time, mac_s, mac_d, ip_s, ip_d, sport, dport, proto, flags, size, modbus_fn, n_pkt_src, n_pkt_dst, modbus_response, label_n, label]

    for t in tqdm(range(len(litemp)), total = len(litemp)):
        for v in range(len(li)):
            if t%len(li)==v:
                li[v].append(litemp[t])
    
    dfphys['mac_s']=mac_s
    dfphys['mac_d']=mac_d
    dfphys['ip_s']=ip_s
    dfphys['ip_d']=ip_d
    dfphys['sport']=sport
    dfphys['dport']=dport
    dfphys['proto']=proto
    dfphys['flags']=flags
    dfphys['size']=size
    dfphys['modbus_fn']=modbus_fn
    dfphys['n_pkt_src']=n_pkt_src
    dfphys['n_pkt_dst']=n_pkt_dst
    dfphys['modbus_response']=modbus_response

    dfphys.to_csv(name)
    return


def preprocessing(mergedfile):
    df = pd.read_csv(mergedfile)

    df = df.drop(columns=df.columns[0:2], axis=1)

    for col in df:
        df.rename(columns = {col:col.lower()}, inplace=True)

    dfX = df.drop(['label', 'label_n', 'n_pkt_src'], axis=1)
    Y = df['label']
    dfX = pd.get_dummies(dfX, columns = ['mac_s', 'mac_d', 'ip_s', 'ip_d', 'proto', 'modbus_fn', 'modbus_response'])
    for col in dfX:
        dfX[col] = dfX[col].fillna(dfX[col].mode()[0])
    return dfX, Y
