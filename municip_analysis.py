# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 15:14:45 2020

@author: tomvi
"""

from ecox import *
from data import agpap, agtender

###
drop=['after321', 'before321','credit314', 'debit314', 'close314','open314']
agpap=agpap.drop(columns=drop)
years = unique_sort(agpap["year"])

# monitor.c_ucjed
from data import munic

mpap=agpap.merge(munic,left_on="ICO_gov",right_on="ico",how="inner").drop(\
              columns="ico")
unique_set=(set(mpap[mpap["year"]=="2015"]["ICO_gov"]) & \
            set(mpap[mpap["year"]=="2016"]["ICO_gov"]) & \
            set(mpap[mpap["year"]=="2017"]["ICO_gov"]) & \
            set(mpap[mpap["year"]=="2018"]["ICO_gov"]))
m_ico=pd.Series(list(unique_set),name="ICO_gov") #451
unique_set_code=(set(mpap[mpap["year"]=="2015"]["m_code"]) & \
            set(mpap[mpap["year"]=="2016"]["m_code"]) & \
            set(mpap[mpap["year"]=="2017"]["m_code"]) & \
            set(mpap[mpap["year"]=="2018"]["m_code"]))
pap_m_code=pd.Series(list(unique_set_code),name="m_code") 

mpap=mpap.merge(pd.Series(list(m_ico),name="ICO_gov"),on="ICO_gov")

mtender=agtender.merge(munic,left_on="ICO_gov",right_on="ico", how="inner")
mtender=mtender.merge(m_ico,on="ICO_gov",how="inner").drop(columns="ico")


c_ico=pd.Series(list(set(pap["ICO_gov"]) & set(agtender["ICO_gov"])))
print(len(c_ico))
tender=tender.merge(municip,left_on="buyer_id",right_on="ico",how="inner")
tender=tender.drop(columns="ico")

print("PAP")
print("Total")
compare(sum,what="credit321",bil=True,years=years,data=mpap)
print("Prague")
compare(sum,mpap[mpap["m_name"]=="Praha"],years,what="credit321",bil=True)

print("Tender")
print("Total")
compare(sum,mtender,years,what="final_price",bil=True)
print("Prague")
compare(sum,mtender[mtender["m_name"]=="Praha"],years,what="final_price",\
        bil=True)

for yr in years:
    print(yr)
    year=agpap["year"]==yr
    result=len(set(agpap[year]["ICO_gov"]))
    print(result)

# elections
from data import elections    
non_elect=0
for code in municip_codes:
    if str(code) not in list(set(elections["code"])):
        non_elect+=1
        print(code)
print(non_elect)

len(set(candidates["partyname"].apply(upper)) & \
    set(winners["party"].apply(upper))) # 1736
notind=candidates["partyshortcut"]!="NK"
len(set(candidates[notind]["partyname"].apply(upper))) # 20442
len(set(candidates["partyname"].apply(upper))) # 41046
len(set(candidates["partyshortcut"].apply(upper))) # 5119
len(set(winners["party"].apply(upper))) # 3412
    

# candidates 
from data import candidates

duplic=candidates[candidates.duplicated(subset=["name","m_code"],keep=False)]
len(set(duplic["name"])) #560
candidates2=candidates.drop_duplicates(subset=["name","m_code"],keep=False)
# -1146 lines


mayors["name"]=mayors["mayor"].apply(upper)
mayors["name2"]=mayors["mayor_inv"].apply(upper)
mayors2=mayors.merge(candidates2,on=["m_code","name"], how="left") #4318
found=mayors2[mayors2["vstrana"]>0]
not_found=mayors[(mayors2["vstrana"]>0).apply(tonot)].drop(columns=["name"])
second_try=not_found.merge(candidates2,left_on=["m_code","name2"],\
                      right_on=["m_code","name"],how="left") # 1099
# only 344 not found

# analysis
mayors_fin=found.append(second_try,sort=True).reset_index(drop=True) # 5762
data=mayors_fin.merge(winners,left_on="m_code",right_on="code",\
                      how="inner").dropna(subset=["vstrana"]) #5418
sum(data["partyc"]=="draw") # 985
data_win=data[data["partyc"]!="draw"] # 4433
data_90=data_win[data_win["partyc"]!="90"] # 1047
sum(data["partyc"]=="90") # 3386


sum(data_win["vstrana"]==data_win["partyc"].apply(int)) # 4278/4433 96.5%
sum(data_90["vstrana"]==data_90["partyc"].apply(int)) # 970/1047 92.8%

# 5,10,20
data5=select(data,"5",True) # 4388
data10=select(data,"10",True) # 4133
data20=select(data,"20",True) # 3280

data590=select(data_90,"5",True) # 1013
data1090=select(data_90,"10",True) # 893
data2090=select(data_90,"20",True) # 654

for datax in [data_win,data5,data10,data20,data_90,data590,data1090,data2090]:
    print(sum(datax["vstrana"]==datax["partyc"].apply(int))/len(datax)) 
    # 92.8%, 93.7%, 95.3%, 98,3%
    # 96.5%, 96.8%, 97.7%, 99.2%

# change in governance
from data import winners,winners14

winners_all=winners.merge(winners14,on="code",suffixes=["_18","_14"],\
                           how="inner")

winners_pap=winners_all.merge(pap_m_code,left_on="code",right_on="m_code",\
                              how="inner").drop(columns="code")

change_list=[]
for code in winners_pap.index:
    record=winners_pap.loc[code]
    if record["partyc_14"]==record["partyc_18"]:
        change="no_change"
    elif record["partyc_18"]=="draw" or record["partyc_14"]=="draw":
        change="one_draw"
    else:
        if record["10_18"]==True:
            change="large_change"
        else:
            change="small_change"
    change_list.append(change)
winners_pap["change"]=change_list # 232-100-119

individual_change={"599921": "large_change","539058": "large_change",\
                    "539333": "large_change","573990": "small_change",\
                    "575071": "large_change","586161": "small_change"}

for key in individual_change.keys():
    winners_pap.loc[winners_pap["m_code"]==key,"change"]=individual_change[key]

#winners_pap.to_csv(loc+"municip\\winners_pap2.csv",index=False)
#winners_pap=pd.read_csv(loc+"municip\\winners_pap2.csv",\
#                                                       dtype={"m_code":"str"})


#######
#######
########


results2=pd.DataFrame(columns=["m_code","year","new_rate","money_rate"])
i=0
year="2019"
unique_full=(set(mpap[mpap["year"]=="2015"]["m_code"]) & \
                set(mpap[mpap["year"]=="2016"]["m_code"]) & \
                set(mpap[mpap["year"]=="2017"]["m_code"]) & \
                set(mpap[mpap["year"]=="2018"]["m_code"]) & \
                set(mpap[mpap["year"]=="2019"]["m_code"])) # -8 451
for municip in unique_full:
    i+=1
    if i % 50==0:
        print(str(int(i/5))+"%")
    is_municip=mpap["m_code"]==municip 
    this_year=mpap["year"]==year
    last_year=(mpap["year"]==string_plus(year,-1)) | (mpap["year"]==\
              string_plus(year,-2)) | (mpap["year"]==string_plus(year,-3)\
                         ) | (mpap["year"]==string_plus(year,-4))
    data=mpap[is_municip & this_year]
    municip_last_year=is_municip & last_year
    firms_ly=pd.DataFrame(list(set(mpap[municip_last_year]["ICO_firm"])),\
                          columns=["ICO_firm"])
    firms_ly["new"]=[False for k in range(len(firms_ly))]
    frame=data.merge(firms_ly,how="left",on="ICO_firm").fillna(True)
    new=frame["new"]==True
    new_rate=len(set(frame[new]["ICO_firm"]))/len(set(frame["ICO_firm"]))
    money_rate=sum(frame[new]["credit321"])/sum(frame["credit321"])
    row=len(results2)
    results2.loc[row]=[municip,year,new_rate,money_rate]
#results2.to_csv(loc+"municip\\results2.csv",index=False)
#results2=pd.read_csv(loc+"municip\\results2.csv",dtype={"m_code":"str"})

frame2=results2.merge(winners_pap[["m_code","change"]],how="left",on="m_code")
frame2=frame2.merge(munic[["m_code","m_name","pop","region_id"]],\
                    how="left",on="m_code")

    

### new elecction approach

# small sample only
elect=elections.merge(pd.Series(pap_m_code,name="code"),on="code",\
                      how="inner")

winners2=pd.DataFrame(columns=["m_code","10","30","50","percentage"])
for municip in set(elect["code"]):
    ismunicip=elect["code"]==municip
    reduced_el=elect[ismunicip]
    criterion=list(reduced_el["percentage"].apply(float))
    criterion.sort(reverse=True)
    if len(criterion)==1:
        party=reduced_el["party"].iloc[0]
        partyc=reduced_el["partyc"].iloc[0]
        ten=False
        thirty=False
        fifty=True
    elif criterion[0]==criterion[1]:
        party="draw"
        partyc="draw"
        ten=False
        thirty=False
        fifty=False
    else:
        share=criterion[0]
        ten=30>share
        thirty=50>share>=30
        fifty=share>=50
    row=len(winners2)
    winners2.loc[row]=[municip,ten,thirty,fifty,criterion[0]]
# winners2.to_csv(loc+"municip\\winners2pap.csv",index=False)

###          
    
for region in list(set(frame2["region_id"])):
    if region!="CZ020":
        frame2[region]=(frame2["region_id"]==region).apply(lambda x: sum([x]))

frame2=frame2.merge(winners2,on="m_code")
for change in set(winners_pap["change"]):
    frame2[change]=(frame2["change"]==change).apply(lambda x: sum([x]))
    print(change)
    print(sum(frame2["change"]==change))
for change in ["10","30","50"]:
    frame2[change]=(frame2[change]*frame2["one_draw"].apply(tonot)*\
          frame2["no_change"].apply(tonot)).apply(int)
    print(change)
    print(sum(frame2[change]))


exo1=frame2[["pop","one_draw","small_change","large_change"\
            ]+[k for k in frame2.columns[8:21]]]

exo2=frame2[["pop","one_draw","10","30","50"\
            ]+[k for k in frame2.columns[8:21]]]
est=OLS(frame2["new_rate"],exo1)
est2=OLS(frame2["money_rate"],exo1)
stargazer = Stargazer([est,est2])
stargazer.render_latex()
white_test(OLS(frame2["new_rate"],exo1,summary=0))
white_test(OLS(frame2["money_rate"],exo1,summary=0))

est=OLS(frame2["new_rate"],exo2)
est2=OLS(frame2["money_rate"],exo2)
stargazer = Stargazer([est,est2])
stargazer.render_latex()
    



from data import pap
for i in unique_sort(pap.day):
    print(i)
    print(len(select(select(pap,"account","321"),"day",i)))

compare(data=agpap,what="credit321",years=unique_sort(agpap.year),sth=sum,\
        bil=True)
compare(data=agpap,what="ICO_gov",years=unique_sort(agpap.year),sth=len,\
        sth2=set)
compare(data=agpap,what="ICO_firm",years=unique_sort(agpap.year),sth=len,\
        sth2=set)


### samples
# large and small

frameS=frame2[frame2["pop"]<=10000]
frameL=frame2[frame2["pop"]>10000]

exoL=frameL[["pop","one_draw","small_change","large_change"]+[\
           k for k in frame2.columns[8:21]]]
exoS=frameS[["pop","one_draw","small_change","large_change"]+[\
           k for k in frame2.columns[8:21]]]
for change in set(frameL.change):
    print(change)
    print(sum(frameL.change==change))

estlarge=OLS(frameL["new_rate"],exoL)
est2large=OLS(frameL["money_rate"],exoL)
estsmall=OLS(frameS["new_rate"],exoS)
est2small=OLS(frameS["money_rate"],exoS)

stargazer = Stargazer([estsmall,est2small,estlarge,est2large])
stargazer.render_latex()



sns.set_style("darkgrid")
def period_name(number):
    number=int(number-4)
    quarter={1:"1",2:"2",3:"3",0:"4"}
    return str(2015+ int((number-1) / 4))+"_"+quarter[number % 4]
munn_frame=mun_frame[mun_frame["year"]!=2014]

### bar chart: https://python-graph-gallery.com/11-grouped-barplot/

# set width of bar
barWidth = 0.25

font = {'size':14,'weight':'normal'}

plt.rc('font', **font)
 
# set height of bar
bars1 = munn_frame["RS321"].apply(lambda x: x/eff_tax)
bars2 = munn_frame["final_price"]
 
# Set position of bar on X axis
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]

 
# Make the plot
plt.bar(r1, bars1, color='#7f5d5f', width=barWidth, edgecolor='white',\
        label='PAP')
plt.bar(r2, bars2, color='#547f2d', width=barWidth, edgecolor='white',\
        label='Tender data')



 
# Add xticks on the middle of the group bars
plt.xlabel('Comparison', fontweight='bold')
plt.xticks([r + barWidth/2 for r in range(len(bars1))], \
            list(munn_frame["period_index"].apply(period_name)))
 
# Create legend & Show graphic
plt.legend()
plt.show()

### schema



#plt.rcdefaults()
fig, ax = plt.subplots()

# Example data
labels = ('Q1', 'Q2', 'Q3', 'Q4')
y_pos = np.arange(len(labels))
performance = [3,6,9,12]

ax.barh(y_pos, performance,color='#7f5d5f', align='center')
ax.set_yticks(y_pos)
ax.set_yticklabels(labels)
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xlabel('Months')


plt.show()

### accounting data delay

### quarterly pap, needs original pap
print(set(pap18["day"].apply(lambda x: x[-4:])))
quarters = {1:"0331",2:"0630",3:"0930",4:"1231"}
inv_quarters={v: k for k, v in quarters.items()}

drop=['account', 'after', 'before','etrziste', 'profil', 'vestnik', 'year']
a321=pap18["account"]=="321"
agpap=aggregate_data(pap18[a321].drop(columns=drop),["ICO_gov","day"])
agpap_mun=agpap.merge(pd.DataFrame(m_ico,columns=["ICO_gov"]),how="right")

frame_columns=["year","quart","period_index","RS321"]
sum_frame=pd.DataFrame(columns=frame_columns)
mun_frame=pd.DataFrame(columns=frame_columns)
for period in unique_sort(pap18["day"]):
    summum=sum(agpap[agpap["day"]==period]["RS"])
    sum_mun=sum(agpap_mun[agpap_mun["day"]==period]["RS"])
    if period[-4:]==quarters[1]:
        final_sum=summum
        final_mun=sum_mun
    else:
        final_sum=summum-last_sum
        final_mun=sum_mun-last_mun
    last_sum=summum
    last_mun=sum_mun
    year=int(period[:4])
    quarter=inv_quarters[period[-4:]]
    to_append=pd.Series([year,quarter,quarter+(year-2014)*4,final_sum],\
                         index=frame_columns)
    to_mun=pd.Series([year,quarter,quarter+(year-2014)*4,final_mun],\
                      index=frame_columns)
    sum_frame=sum_frame.append(to_append,ignore_index=True)
    mun_frame=mun_frame.append(to_mun,ignore_index=True)
    
### quarterly tender, needs original tender

tender=tender.filter(["buyer_id","award_date","final_price"])
tender=tender[tender["award_date"].apply(lambda x: type(x))==str]

def date_day(date):
    return date[:4] + quarters[math.floor((int(date[5:7])+2)/3)]

tender.loc[:,"day"] = tender["award_date"].apply(date_day)
tender_mun=tender.merge(pd.DataFrame(m_ico,columns=["ICO_gov"]),\
                        left_on="buyer_id",right_on="ICO_gov",how="right")

def period(period):
    quarter=inv_quarters[period[-4:]]
    year=int(period[:4])
    return quarter+(year-2014)*4
quarter_tender=aggregate_data(tender,["day"])
quarter_munt=aggregate_data(tender_mun,["day"])
quarter_tender.loc[:,"period_index"]=quarter_tender["day"].apply(period)
quarter_munt.loc[:,"period_index"]=quarter_munt["day"].apply(period)



for data in [quarter_tender,quarter_munt]:
    for i in range(1,24):
        data.loc[:,"final_price_"+str(i)]=data["final_price"].shift(i)
        
        
sum_frame=sum_frame.merge(quarter_tender,how="left",on=["period_index"])
mun_frame=mun_frame.merge(quarter_munt,how="left",on=["period_index"])



# mun_frame=pd.read_excel("D:\\dir\\skola\\Bakalářka\\výsledky\\mun_frame.xlsx")
# mun_frame=mun_frame[mun_frame["year"]!=2014]
    
np.corrcoef(mun_frame["RS321"],mun_frame["final_price_1"])

model=["final_price" + k for k in [""]+["_"+str(i) for i in range(1,6)]]
eff_tax=1+3/4*0.21+1/4*((0.10+0.15)/2)
stargazer = Stargazer([OLS(mun_frame["RS321"].apply(lambda x: x/eff_tax),\
    mun_frame[model])])
stargazer.render_latex()


