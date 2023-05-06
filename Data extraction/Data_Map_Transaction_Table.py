# INITIALIZATION
import os
from os import walk
from pathlib import Path
import pandas as pd
Data_Map_Transaction_Table = pd.DataFrame({})

def Data_Map_Transaction_Table_Fun(state,year,quarter,path):
    global Data_Map_Transaction_Table
    dfmt = pd.read_json(path)
    DMT_temp=dfmt['data']['hoverDataList'] 
    if DMT_temp:
        for i in DMT_temp:
            DMT_row={ 'Place Name':i['name'], 'Total Transactions count':i['metric'][0]['count'], 'Total Amount':i['metric'][0]['amount'],'Quarter':quarter,'Year': year,'State':state}  
            Data_Map_Transaction_Table = Data_Map_Transaction_Table._append(DMT_row, ignore_index = True)  

t_s= r'F:\Python\Projects\Project 1\reference\pulse-master\pulse-master\data\map\transaction\hover\country\india\state'
t_path = r'F:\Python\Projects\Project 1\reference\pulse-master\pulse-master\data\map\transaction\hover\country\india\state'
t_states = os.listdir(t_path) 

for i in t_states:
    #print(i)
    p=t_s+'\\'+i
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
            Data_Map_Transaction_Table_Fun(i,j,fn,fp)
            #print(fp) 

Data_Map_Transaction_Table.to_csv('Data_Map_Transaction_Table.csv',index=False) 
print(len(Data_Map_Transaction_Table))
Data_Map_Transaction_Table.head(5)