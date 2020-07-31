# -*- coding: utf-8 -*-
"""
Created on Sun Jun 28 17:03:10 2020

@author: tomvi
"""

from ecox import *
loc="D:\\dir\\Python\\data\\"
# PAP

### creating one dataset 
#cannot state dtype for float as they are in wrong format - will be fixed later
dtype={"year": "str","day":"str","ICO_gov":"str","ICO_firm":"str",\
       "account":"str","etrziste":"str","profil":"str","vestnik":"str"}
columns=["year","day","ICO_gov","ICO_firm","account","etrziste","profil", \
         "vestnik", "open", "debit","credit","close"]
#not all files have columns in the same order!
columns2=["year","day","ICO_gov","account","ICO_firm","etrziste","profil", \
         "vestnik", "open", "debit","credit","close"] 

#perhaps you will have different names of files
pap = pd.read_csv("D:\\dir\\Python\\data\\pap\\PAP10_2014.csv", \
                     names=columns, sep = ";",encoding="mbcs", header=0, \
                     dtype=dtype)

# 2019 only one file, first 3 quarters missing
file_names=[]
pre="D:\\dir\\Python\\data\\pap\\PAP10_"
for i in ["15","16","17","18"]:
    for j in ["003","006","009","012"]:
        file_names.append(pre + "20" + i + j + ".csv")
file_names.append(pre+"2019012.csv")

for name in file_names:
    pap = pap.append(pd.read_csv(name,names=columns2,sep=";", dtype=dtype,\
                                 encoding="mbcs",header=0,low_memory=False),\
    sort=True)

### negative values
replace={r'(.*?)-':r'-\1'}
what_to_replace={k:replace for k in ["debit","credit","close","open"]}
pap=pap.replace(what_to_replace, regex=True)

pap.to_csv("D:\\dir\\Python\\data\\pap\\PAP10_2014-2019", index=False)
# from now load with dtypes from data.py

# number of partners
years = unique_sort(pap["year"])

by=["ICO_firm","ICO_gov","year"]
unique=pap[by].groupby(by).size().reset_index().drop(columns=0)
by=["ICO_firm","year"]
name={0:"links"}
links_years=unique.groupby(by).size().reset_index().rename(columns=name)

by=["ICO_firm","ICO_gov"]
unique_total=pap[by].groupby(by).size().reset_index().drop(columns=0)
name={0:"links"}
by=["ICO_firm"]
links_total=unique_total.groupby(by).size().reset_index().rename(columns=name)

links=links_total
for yr in years:
    year=links_years["year"]==yr
    merge=links_years[year][["ICO_firm","links"]]
    suff=("","_"+yr)
    links=links.merge(merge,how="outer",on="ICO_firm",suffixes=suff).fillna(0)
links.to_csv("D:\\dir\\Python\\data\\pap\\PAPlinks",index=False)

# agpap
# only 321 and 314 in the data
pap321=pap[pap["account"]=="321"]
pap314=pap[pap["account"]=="314"]
on=['ICO_firm', 'ICO_gov','day','etrziste', 'profil', 'vestnik', 'year']
pap=pap321.merge(pap314,how="outer",on=on,suffixes=("321","314")).fillna(0)
pap=pap.drop(["account321","account314"],axis=1)



def toisx(self):
    return self!="none"
pap["isvestnik"]=pap["vestnik"].apply(toisx)
pap["isprofil"]=pap["profil"].apply(toisx)
pap["isetrziste"]=pap["etrziste"].apply(toisx)
# different aggregation methods, not all used in this work
by0=["ICO_firm","ICO_gov","year","isvestnik","isprofil","isetrziste"]
by=["ICO_firm","year","isvestnik","isprofil","isetrziste"]
bymid=["ICO_firm","ICO_gov","year"]
byfull=["ICO_firm"]
year_end=pap["day"].apply(lambda x: x[-4:])=="1231"

grouped_pap = pap[year_end].groupby(by0,as_index=False)
agpap = grouped_pap.sum().reset_index(drop=True)
agpap.to_csv("D:\\dir\\Python\\data\\pap\\PAP10_2014-2019_ag0", index=False)

grouped_pap = pap[year_end].groupby(by,as_index=False)
agpap = grouped_pap.sum().reset_index(drop=True)
agpap.to_csv("D:\\dir\\Python\\data\\pap\\PAP10_2014-2019_ag", index=False)


grouped_pap = pap[year_end].groupby(bymid,as_index=False)
drop=["isvestnik","isprofil","isetrziste"]
agpap = grouped_pap.sum().reset_index(drop=True).drop(drop,axis=1)
agpap.to_csv("D:\\dir\\Python\\data\\pap\\PAP10_2014-2019_agmid", index=False)

# 2014 probably biased, for total sums not used
no2014=pap["year"]!="2014"
grouped_pap = pap[no2014 & year_end].groupby(byfull,as_index=False)
agpap = grouped_pap.sum().reset_index(drop=True).drop(drop,axis=1)
agpap.to_csv("D:\\dir\\Python\\data\\pap\\PAP10_2014-2019_agfull", index=False)

# tender data agg

def year4(self):
    if type(self)==float:
        return("none")
    else:
        return(self[:4])
tender["year"]=tender["award_date"].apply(year4)

by0=["buyer_id","bidder_id","year"]
by=["bidder_id","year"]

agtender=aggregate_data(tender,by0).drop(columns="bids_count")
agtender.to_csv("D:\\dir\\Python\\data\\zakazky\\zakazky_02-2020_ag0",\
                index=False)
agtender=aggregate_data(tender,by).drop(columns="bids_count")
agtender.to_csv("D:\\dir\\Python\\data\\zakazky\\zakazky_02-2020_ag",\
                index=False)





#elections
#example of one party information
#{'POR_STR_HLAS_LIST': '1', 'VSTRANA': '90', 'NAZEV_STRANY': 'Náš venkov', 
#'HLASY': '679', 'HLASY_PROC': '15.16', 'KANDIDATU_POCET': '9', 
#'ZASTUPITELE_POCET': '1', 'ZASTUPITELE_PROC': '11.11'}

# codebook of all municip, can be used one from 
# https://monitor.statnipokladna.cz/ or any other, I do not remember where
# this one is from
municip_codes=pd.read_csv(loc+"ciselnik_obci.csv",encoding="mbcs")["CHODNOTA"]

base_url="https://volby.cz/pls/kv2018/vysledky_obec?datumvoleb=20181005&"
elections=pd.DataFrame(columns=["code","name","party","percentage","partyc"])
for municip in municip_codes:
    xml = urlopen(base_url+"cislo_obce="+str(municip))
    root = ET.parse(xml).getroot()
    for result in root.iter('{http://www.volby.cz/kv/}OBEC'):
        code=result.attrib["KODZASTUP"]
        name=result.attrib["NAZEVZAST"]
    for result in root.iter('{http://www.volby.cz/kv/}VOLEBNI_STRANA'):
        party=result.attrib['NAZEV_STRANY']
        partyc=result.attrib["VSTRANA"]
        percentage=result.attrib['ZASTUPITELE_PROC']
        row=len(elections)
        elections.loc[row]=[code,name,party,percentage,partyc]
elections.to_csv(loc+"municip\\elections.csv",index=False)

base_url="https://volby.cz/pls/kv2014/vysledky_obec?datumvoleb=20141010&"
elections14=pd.DataFrame(columns=["code","name","party","percentage","partyc"])
for municip in municip_codes:
    xml = urlopen(base_url+"cislo_obce="+str(municip))
    root = ET.parse(xml).getroot()
    for result in root.iter('{http://www.volby.cz/kv/}OBEC'):
        code=result.attrib["KODZASTUP"]
        name=result.attrib["NAZEVZAST"]
    for result in root.iter('{http://www.volby.cz/kv/}VOLEBNI_STRANA'):
        party=result.attrib['NAZEV_STRANY']
        partyc=result.attrib["VSTRANA"]
        percentage=result.attrib['ZASTUPITELE_PROC']
        row=len(elections14)
        elections14.loc[row]=[code,name,party,percentage,partyc]
elections14.to_csv(loc+"municip\\elections14.csv",index=False)
        
winners=pd.DataFrame(columns=["code","party","partyc","5","10","20"])
for municip in set(elections["code"]):
    ismunicip=elections["code"]==municip
    reduced_el=elections[ismunicip]
    criterion=list(reduced_el["percentage"].apply(float))
    criterion.sort(reverse=True)
    if len(criterion)==1:
        party=reduced_el["party"].iloc[0]
        partyc=reduced_el["partyc"].iloc[0]
        five=True
        ten=True
        twenty=True
    elif criterion[0]==criterion[1]:
        party="draw"
        partyc="draw"
        five=False
        ten=False
        twenty=False
    else:
        line=reduced_el["percentage"].apply(float)==criterion[0]
        party=reduced_el[line]["party"].iloc[0]
        partyc=reduced_el[line]["partyc"].iloc[0]
        five=criterion[0]-criterion[1]>=5
        ten=criterion[0]-criterion[1]>=10
        twenty=criterion[0]-criterion[1]>=20
    row=len(winners)
    winners.loc[row]=[municip,party,partyc,five,ten,twenty]
winners.to_csv(loc+"municip\\winners.csv",index=False)

winners14=pd.DataFrame(columns=["code","party","partyc","5","10","20"])
for municip in set(elections14["code"]):
    ismunicip=elections14["code"]==municip
    reduced_el=elections14[ismunicip]
    criterion=list(reduced_el["percentage"].apply(float))
    criterion.sort(reverse=True)
    if len(criterion)==1:
        party=reduced_el["party"].iloc[0]
        partyc=reduced_el["partyc"].iloc[0]
        five=True
        ten=True
        twenty=True
    elif criterion[0]==criterion[1]:
        party="draw"
        partyc="draw"
        five=False
        ten=False
        twenty=False
    else:
        line=reduced_el["percentage"].apply(float)==criterion[0]
        party=reduced_el[line]["party"].iloc[0]
        partyc=reduced_el[line]["partyc"].iloc[0]
        five=criterion[0]-criterion[1]>=5
        ten=criterion[0]-criterion[1]>=10
        twenty=criterion[0]-criterion[1]>=20
    row=len(winners14)
    winners14.loc[row]=[municip,party,partyc,five,ten,twenty]
winners14.to_csv(loc+"municip\\winners14.csv",index=False)



###
# list of mayors cleaned and assigned an identifier in MS excel
columns=["jmeno","prijmeni","kodzastup20180","vstrana20180"]
candidates=pd.read_stata(loc+"municip\\stata_starosta.dta",\
                         preserve_dtypes=False)[columns]
candidates["name"]=(candidates["jmeno"] + candidates["prijmeni"]).apply(upper)
has_code=candidates["kodzastup20180"]>0
candidates["m_code"]=candidates[has_code]["kodzastup20180"\
          ].apply(lambda x: str(int(x)))
candidates=candidates[has_code].drop(columns=["kodzastup20180"])
candidates.drop_duplicates().to_csv(loc+"municip\\candidates.csv",index=False)












