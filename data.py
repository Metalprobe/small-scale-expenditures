# -*- coding: utf-8 -*-
"""
Created on Sun Jun 28 13:33:15 2020

@author: tomvi
"""

import pandas as pd
loc="D:\\dir\\Python\\data\\"


mayors=pd.read_excel(loc+"municip\\mayor2.xlsx",dtype={"m_code":"str"})
links=pd.read_csv(loc+"pap\\PAPlinks")
munic=pd.read_csv(loc+"municip\\municip.csv",dtype={"ico":"str",\
                                                    "m_code":"str"})
elections=pd.read_csv(loc+"municip\\elections.csv",dtype={"code":"str"})
winners=pd.read_csv(loc+"municip\\winners.csv",dtype={"code":"str"})
winners2=pd.read_csv(loc+"municip\\winners2pap.csv",dtype={"m_code":"str"})
winners14=pd.read_csv(loc+"municip\\winners14.csv",dtype={"code":"str"})
names=['jmeno', 'prijmeni', 'vstrana','name', 'm_code']
candidates=pd.read_csv(loc+"municip\\candidates.csv",names=names,header=0,\
                       dtype={"m_code":"str"})
fillvalues = {'buyer_id':'none', 'bidder_id':'none','notice_url':'none', \
              'award_url':'none','final_price':0, \
              'estimated_price':0}
names=['title', 'procedure_type', 'buyer_name', 'buyer_id', 'bidder_name',\
       'bidder_id', 'bid_deadline', 'selection_method', 'is_eu_funded',\
       'bids_count', 'notice_date_first', 'notice_url', 'award_date',\
       'award_url', 'cpv_category', 'final_price',\
       'estimated_price', 'cancellation_date']
tender = pd.read_csv(loc+"zakazky\\zakazky_02-2020.csv", \
                     low_memory=False,names=names,header=0).fillna(fillvalues) 
dtype={"buyer_id":"str","bidder_id":"str","year":"str",\
       "final_price":"float64","estimated_price":"float64"}
columns=['ICO_gov', 'ICO_firm', 'year', 'final_price','estimated_price']
path=loc+"zakazky\\zakazky_02-2020_ag0.csv"
agtender=pd.read_csv(path,dtype=dtype,names=columns,header=0)
dtype={"year": "str","day":"str","ICO_gov":"str","ICO_firm":"str",\
       "account":"str","before":"float64", "LS":"float64","RS":"float64", \
       "after":"float64", "etrziste":"str","profil":"str","vestnik":"str"}
path=loc+"pap\\PAP10_2014-2019"
pap=pd.read_csv(path,dtype=dtype).fillna("none")
path=loc+"pap\\PAP10_2014-2018_v3" #old, not including 2019
pap18=pd.read_csv(path,dtype=dtype).fillna("none")
path=loc+"pap\\PAP10_2014-2019_agmid.csv"
agpap=pd.read_csv(path,dtype=dtype).fillna("none")
path=loc+"pap\\PAP10_2014-2019_ag0"
ag0pap=pd.read_csv(path,dtype=dtype).fillna("none")