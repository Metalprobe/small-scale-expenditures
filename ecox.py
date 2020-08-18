# -*- coding: utf-8 -*-
"""
Created on Sun Jun 28 13:06:28 2020

@author: tomvi
"""

import pandas as pd
import math
import statistics as stat
import statsmodels.api as sm
from statsmodels.stats.diagnostic import het_white as white, \
    het_breuschpagan as bpt
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from stargazer.stargazer import Stargazer


# general functions
def upper(self):
    if type(self)==str:
        return self.upper()
    else:
        return self
def log_0(x):
    if x<=0:
        return 0
    else:
        return math.log(x)
def log(x,a=math.e,zero=True):
    if zero==False:
        return math.log(x)/math.log(a)
    if zero==True:
        return log_0(x)/math.log(a)
def select(df,column,value):
    return df[df[column]==value]
def identity(x):
    return x
def unique_sort(list_):
    final_list = list(set(list_))
    final_list.sort()
    return final_list
def tonot(x):
    return not x
def OLS(endo, exo, c="c", summary=1):
    if c == "c":
        model = sm.OLS(endo, sm.add_constant(exo)).fit()
    else:
        model = sm.OLS(endo,exo).fit()
    if summary == 1:
        print(model.summary())
    return(model)
def white_test(model,printed=False):
    coef=white(model.resid,model.model.exog)
    if printed==True:
        print(coef)
    return(coef)
def bp_test(model,printed=False):
    coef=bpt(model.resid,model.model.exog)
    if printed==True:
        print(coef)
    return(coef)
def aggregate_data(data,by):
    grouped_data=data.groupby(by=by,as_index=False)
    return grouped_data.sum().reset_index(drop=True)  
def string_plus(self,x=1):
    return str(int(self)+x)
def fill_ico(self):
    ap="00000000"
    full=ap+str(self)
    return full[-8:]
def compare(sth,data,years,sth2=identity,restrict=False,bil=False,\
            what="RS321"):
    global compare_out
    print(str(sth))
    compare_table=[]
    if bil==False:
        bil_str=" = "
        bil=1
    else:
        bil_str=" in bilions = "
        bil=10**9
    for yr in unique_sort(years):
        year=data["year"]==yr
        if type(restrict)==bool:
            when=year
        else:
            when=year & restrict
        if sth==sum:
            result=sth(sth2(data[when][what]/bil))
        else:
            result=sth(sth2(data[when][what]))
        print("Result for " \
          + yr + bil_str + str(result))
        compare_table.append(result)
    compare_out=compare_table