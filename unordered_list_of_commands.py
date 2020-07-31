# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 12:29:15 2019

@author: tomvi
"""

### rather a toolbox, most of it was not used
from ecox import *
from data import # what you need

### compare years - needs agpap

vest=agpap["isvestnik"]==True
etr=agpap["isetrziste"]==True
profil=agpap["isprofil"]==True
small=vest.apply(tonot) & etr.apply(tonot) & profil.apply(tonot)
compare(sum,restrict=small,mld=True)

compare(sum,mld=True)
compare(len)

compare(sum,data=agtender,mld=True,what="final_price",years=agtender["year"])
compare(len,data=agtender,years=agtender["year"],what="final_price")

# LS x RS

OLS(agpap["LS321"],agpap["RS321"])
np.corrcoef(agpap["LS321"],agpap["RS321"])


### tender data lag
    
# aggregate by contractors
drop=['katobyv_id', 'obec','kraj', 'pocob']
non_valid=["00000000","00000027","00000019","00000001","00000111","00000444"]
valid_ico=pap["ICO_firm"].apply(lambda x: x not in non_valid)
firm=pap["ICO_firm"].apply(lambda x: len(x)>2)
valid_firm=valid_ico & firm
agpap=aggregate_data(pap[valid_firm].drop(columns=drop),["ICO_firm","year"])

agtender=aggregate_data(tender.drop(columns=drop),["ICO_firm","year"])
 
# merge

compareframe=agpap.merge(agtender,how="left",on=["ICO_firm","year"]).fillna(0)

# panel

comparepanel=compareframe
agtenderlag=agtender[agtender["year"]!="none"]
yearname="year"
for i in range(1,8):
    lastname=yearname
    yearname="year_" + str(i)
    agtenderlag.loc[:,yearname]=agtenderlag[lastname].apply(string_plus)
    columns=["ICO_firm",yearname,"final_price"]
    left=["ICO_firm","year"]
    right=["ICO_firm",yearname]
    suff=("","_"+str(i))
    comparepanel=comparepanel.merge(agtenderlag[columns],how="left",\
                                  left_on=left,right_on=right,suffixes=suff)
    comparepanel=comparepanel.drop(columns=["year_"+str(i)])
comparepanel=comparepanel.fillna(0)

data=comparepanel
for i in range(8):
    print(str(i))
    if i==0:
        print(str(np.corrcoef(data["RS321"],data["final_price"])[0,1]))
    else:
        print(str(np.corrcoef(data["RS321"],data["final_price_"+str(i)])[0,1]))
        
# LS ~ final_price

OLS(comparepanel["LS321"],comparepanel["final_price"])
np.corrcoef(comparepanel["LS321"],comparepanel["final_price"])
compare(sum,data=comparepanel,bil=True,what="LS321")
compare(sum,data=comparepanel,bil=True,what="final_price")

# LS ~ sum(final_price_t_i)

endo=comparepanel["RS321"]
lags=["final_price","final_price_1","final_price_2","final_price_3",\
      "final_price_4","final_price_5"]
exo=comparepanel[lags]

sum(OLS(endo,exo).params[1:])
compare(sum,data=comparepanel,bil=True,what="LS321")
compare(sum,data=comparepanel,bil=True,what="final_price_3")





vest=agpap2["isvestnik"]==True
etr=agpap2["isetrziste"]==True
profil=agpap2["isprofil"]==True
small=vest.apply(tonot) & etr.apply(tonot) & profil.apply(tonot)
restrict_list = [vest,etr,profil,small,False]
for restrict in restrict_list:
    compare(sum,restrict=restrict,bil=True,data=agpap2)


### links need PAPlinks

for column in filter(lambda x:x!="ICO_firm",links.columns):
    print(column)
    print(stat.median(links[column]))
    print(stat.stdev(links[column]))


plt.hist(links["links"],color="tab:red",range=(0,10))
plt.hist(links["links"],color="tab:red",range=(100,1000))
plt.hist(links["links"].apply(log,args=(2,)),color="tab:red")
sns.boxplot(links["links"])
plt.gca().set(title="Hist")
plt.show()



#########################

results=pd.DataFrame(columns=["m_code","year","new_rate","money_rate"])
i=0
unique_full=(set(mpap[mpap["year"]=="2015"]["m_code"]) & \
                set(mpap[mpap["year"]=="2016"]["m_code"]) & \
                set(mpap[mpap["year"]=="2017"]["m_code"]) & \
                set(mpap[mpap["year"]=="2018"]["m_code"]) & \
                set(mpap[mpap["year"]=="2019"]["m_code"])) # -8 451
for municip in unique_full:
    for year in ["2016","2017","2018","2019"]:
        i+=1
        if i % 180==0:
            print(str(int(i/18))+"%")
        is_municip=mpap["m_code"]==municip 
        this_year=mpap["year"]==year
        last_year=mpap["year"]==string_plus(year,-1)
        data=mpap[is_municip & this_year]
        municip_last_year=is_municip & last_year
        firms_ly=pd.DataFrame(list(set(mpap[municip_last_year]["ICO_firm"])),\
                              columns=["ICO_firm"])
        firms_ly["new"]=[False for k in range(len(firms_ly))]
        frame=data.merge(firms_ly,how="left",on="ICO_firm").fillna(True)
        new=frame["new"]==True
        new_rate=len(set(frame[new]["ICO_firm"]))/len(frame["ICO_firm"])
        money_rate=sum(frame[new]["RS321"])/sum(frame["RS321"])
        row=len(results)
        results.loc[row]=[municip,year,new_rate,money_rate]
results.to_csv(loc+"municip\\results.csv",index=False)

frame=results.merge(winners_pap[["m_code","change"]],how="left",on="m_code")
frame=frame.merge(munic[["m_code","pop"]],how="left",on="m_code")

        
for year in set(frame["year"]):
    frame[year]=(frame["year"]==year).apply(lambda x: sum([x]))
frame["2019*draw"]=(frame["change"]==1).apply(lambda x: sum([x]))*frame["2019"]
frame["2019*new_governance"]=(frame["change"]==2).apply(lambda x: sum([x])\
     )*frame[\
     "2019"]
exo=frame[["pop",2019*new_governance","2019*draw"]]
OLS(frame["money_rate"],exo)






















