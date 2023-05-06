#importing necessary libraries
import os
from os import walk
from pathlib import Path
import pandas as pd

#creating empty dataframe to store the extracted data 
Aggregate_transaction_table = pd.DataFrame({})
Aggregate_transaction_Summary_table = pd.DataFrame({})
#creating global variable to access the variables outside the function
def Aggregated_Transaction(state,year,quarter,path):
    global Aggregate_transaction_table
    global Aggregate_transaction_Summary_table
    dft = pd.read_json(path)

    datafrom = dft['data'] ['from']
    datato=dft['data'] ['to']
    T_row={'State':state,'Year':year,'Quarter':quarter,'Data From':datafrom,'Data To':datato}
    Aggregate_transaction_Summary_table=Aggregate_transaction_Summary_table._append(T_row,ignore_index = True)

    DAT_temp=dft['data']['transactionData']
    if DAT_temp:      
        for i in DAT_temp:
            DAT_row={ 'Payment Mode':i['name'], 'Total Transactions count':i['paymentInstruments'][0]['count'], 'Total Amount':i['paymentInstruments'][0]['amount'],'Quarter':quarter,'Year': year,'State':state}  
            Aggregate_transaction_table = Aggregate_transaction_table._append(DAT_row, ignore_index = True)
        
# PATH FOR ALL STATES IN AGGREGATED TRANSACTIONS
t_s= r'F:\Python\Projects\Project 1\reference\pulse-master\pulse-master\data\aggregated\transaction\country\india\state'
t_path = r'F:\Python\Projects\Project 1\reference\pulse-master\pulse-master\data\aggregated\transaction\country\india\state'
t_states = os.listdir(t_path) # NAMES OF ALL DIRECTORIES IN STATES (36 STATES)
for i in t_states:                     # ITERATE ALL STATES
    #print(i)
    p=t_s+'\\'+i                       # PICK ONE STATE PATH
    states_year=os.listdir(p)          # PICK 2018 TO 2022 DIRECTORIES IN ONE STATE
    for j in states_year:              # ITERATE YEARS 2018 TO 2022
        #print(j)
        pt=p+'\\'+j                    # PICK ONE YEAR PATH
        f=[]
        for (dirpath, dirnames, filenames) in walk(pt):
            f.extend(filenames)        # PICK ALL THE QFILES IN SELECTED YEAR 
            break
        for k in f:                    # ITERATE THROUGH QFILES 1.JSON TO 4.JSON
            fp=pt+'\\'+k               # PICK ONE QFILE PATH
            fn=Path(fp).stem           # NOTE DOWN QUARTER
            #print(i,j,fn)
            Aggregated_Transaction(i,j,fn,fp) 
            #print(fp) 
#Data_Aggregate_transaction_table
Aggregate_transaction_table.to_csv('Aggregate_transaction_table.csv',index=False)
print(len(Aggregate_transaction_table))
Aggregate_transaction_table.head(5)
#Data_Aggregated_Transaction_Summary_Table
Aggregate_transaction_Summary_table.to_csv('Aggregate_transaction_Summary_tablee.csv',index=False)
print(len(Aggregate_transaction_Summary_table))
Aggregate_transaction_Summary_table.head(5)