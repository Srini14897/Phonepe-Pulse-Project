# INITIALIZATION
import os
from os import walk
from pathlib import Path
import pandas as pd

Data_Aggregated_User_Table = pd.DataFrame({}) 
Data_Aggregated_User_Summary_Table = pd.DataFrame({}) 


def Aggregated_User_Table_fun(state,year,quarter,path):
    global Data_Aggregated_User_Table
    global Data_Aggregated_User_Summary_Table
    dfu = pd.read_json(path)

    registeredUsers=dfu['data']['aggregated']['registeredUsers']
    appOpens=dfu['data']['aggregated']['appOpens']
    U_row={'State':state,'Year': year,'Quarter':quarter,'Registered Users':registeredUsers,'AppOpenings':appOpens}
    Data_Aggregated_User_Summary_Table= Data_Aggregated_User_Summary_Table._append(U_row, ignore_index = True )

    DAU_temp=dfu['data']['usersByDevice']
    if DAU_temp:  
        for i in DAU_temp:
            DAU_row={ 'Brand Name':i['brand'], 'Registered Users Count':i['count'], 'Percentage Share of Brand':i['percentage'],'Quarter':quarter,'Year': year,'State':state}  
            Data_Aggregated_User_Table = Data_Aggregated_User_Table._append(DAU_row, ignore_index = True)

s= r'F:\Python\Projects\Project 1\reference\pulse-master\pulse-master\data\aggregated\user\country\india\state'
path = r'F:\Python\Projects\Project 1\reference\pulse-master\pulse-master\data\aggregated\user\country\india\state'
states = os.listdir(path)

for i in states:
    #print(i)
    p=s+'\\'+i
    states_year=os.listdir(p)
    for j in states_year:
        #print(j)
        pt=p+'\\'+j
        f=[]
        for (dirpath, dirnames, filenames) in walk(pt):
            f.extend(filenames)
            break
        for k in f:
            fp=pt+'\\'+k            
            fn=Path(fp).stem
            #print(i,j,fn)
            Aggregated_User_Table_fun(i,j,fn,fp)
            #print(fp)  
            Data_Aggregated_User_Table.to_csv('Data_Aggregated_User_Table.csv',index=False) 
            print(len(Data_Aggregated_User_Table))
            Data_Aggregated_User_Table.head(5)

            Data_Aggregated_User_Summary_Table.to_csv('Data_Aggregated_User_Summary_Table.csv',index=False) 
            print(len(Data_Aggregated_User_Summary_Table))
            Data_Aggregated_User_Summary_Table.head(5)