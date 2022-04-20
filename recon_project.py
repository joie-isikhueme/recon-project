from collections import OrderedDict as od
import pandas as pd
import fuzzywuzzy
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import pyodbc
import sqlalchemy


df1 = pd.read_excel(r"C:\Users\jisikhueme003\Documents\Recon-FMN\Zenith_Ledger_January.xlsx",engine = 'openpyxl',sheet_name='Sheet1')

d1 = df1[["Date","Description","Amount"]]

df2 = pd.read_excel(r"C:\Users\jisikhueme003\Documents\Recon-FMN\Zenith_Statement_January.xlsx", engine ='openpyxl',sheet_name='Sheet1')

df2["Credit Amount"] = df2["Credit Amount"].fillna(0)

df2["Debit Amount"] = df2["Debit Amount"].fillna(0)

df2["Amount"] = df2["Credit Amount"] + df2["Debit Amount"]

d2 = df2[["Effective Date","Description","Amount"]]


d2.columns=["Date_y","Description_y","Amount_y"]

ledger=d1.copy()
ledger['count']=ledger.groupby('Description').cumcount()

bs=d2.copy()
bs['count_y']=bs.groupby('Description_y').cumcount()

merged= pd.merge(ledger,bs,left_on=['Date','Description','Amount','count'],right_on=['Date_y','Description_y','Amount_y','count_y'],how='inner')
merged_ledger=merged.copy()
merged_ledger.drop(['Date_y','Description_y','Amount_y','count_y'],axis=1,inplace=True)

ledger_remaining=ledger.append(merged_ledger).reset_index(drop=True)
ledger_remaining['Duplicated']=ledger_remaining.duplicated(keep=False)
ledger_remaining=ledger_remaining[~ledger_remaining['Duplicated']]


merged_bs=merged.copy()
merged_bs.drop(['Date','Description','Amount','count'],axis=1,inplace=True)

bs_remaining=bs.append(merged_bs).reset_index(drop=True)
bs_remaining['Duplicated']=bs_remaining.duplicated(keep=False)
bs_remaining=bs_remaining[~bs_remaining['Duplicated']]



probable_match=pd.merge(ledger_remaining,bs_remaining,left_on=['Description','Amount','count'],right_on=['Description_y','Amount_y','count_y'],how='inner')


probable_match_ledger=probable_match.copy()
probable_match_ledger.drop(['Date_y','Description_y','Amount_y','count_y','Duplicated_y'],axis=1,inplace=True)
probable_match_ledger.rename({'Duplicated_x':'Duplicated'},axis=1,inplace=True)


ledger_remaining=ledger_remaining.append(probable_match_ledger)
ledger_remaining.drop(['Duplicated'],axis=1,inplace=True)
ledger_remaining['Duplicated']=ledger_remaining.duplicated(keep=False)
ledger_remaining=ledger_remaining[~ledger_remaining['Duplicated']]


probable_match_bs=probable_match.copy()
probable_match_bs.drop(['Date','Description','Amount','count','Duplicated_x'],axis=1,inplace=True)
probable_match_bs.rename({'Duplicated_y':'Duplicated'},axis=1,inplace=True)


bs_remaining=bs_remaining.append(probable_match_bs)
bs_remaining.drop(['Duplicated'],axis=1,inplace=True)
bs_remaining['Duplicated']=bs_remaining.duplicated(keep=False)
bs_remaining=bs_remaining[~bs_remaining['Duplicated']]


ledger_remaining['matched_description']=ledger_remaining['Description'].apply(lambda x: process.extractOne(x, bs_remaining['Description_y'].to_list(),score_cutoff=87))
try:
    ledger_remaining['Description_New'],ledger_remaining['Match_Percent'] = ledger_remaining['matched_description'].str[0],ledger_remaining['matched_description'].str[1]
except Exception:
    pass

ledger_remaining['matched_description']=ledger_remaining['matched_description'].fillna('-')

matched=["no_match"]
ledger_remaining['filter1']=ledger_remaining['matched_description'].map({'-':"no_match"})
probable_match2 = ledger_remaining.loc[~ledger_remaining.filter1.isin(matched)]

ledger_remaining= ledger_remaining.loc[ledger_remaining.filter1.isin(matched)]
ledger_remaining= ledger_remaining.drop([
                                        'Duplicated','matched_description','Description_New',
                                        'Match_Percent','filter1'],axis=1)




probable_match2['count_desc_y']=probable_match2.groupby('Description_New').cumcount()


probable_match2_new=pd.merge(probable_match2,bs_remaining,left_on=['Description_New','count_desc_y'],right_on=['Description_y','count_y'],how='left')


probable_match2_bs=probable_match2_new.copy()
probable_match2_bs.drop([
                        'Date',
                        'Description',
                        'Amount',
                        'count',
                        'Duplicated_x',
                        'matched_description',
                        'Description_New',
                        'Match_Percent',
                        'filter1',
                        'count_desc_y',
                        'Duplicated_y'
                        ],axis=1,inplace=True)
# probable_match2_bs.rename({'Duplicated_y':'Duplicated'},axis=1,inplace=True)
probable_match2_bs.dropna(inplace=True)


bs_remaining.drop(['Duplicated'],axis=1,inplace=True)


bs_remaining=bs_remaining.append(probable_match2_bs).reset_index(drop=True)
# bs_remaining.drop(['Duplicated'],axis=1,inplace=True)
# bs_remaining2['Duplicated']=bs_remaining.duplicated(keep=False)
# bs_remaining2=bs_remaining[~bs_remaining['Duplicated']]
# bs_remaining= bs_remaining.drop(['Duplicates'],axis=1)
bs_remaining=bs_remaining.drop_duplicates(keep=False)


ledger_remaining['matched_description']=ledger_remaining['Description'].apply(lambda x: process.extractOne(x, bs_remaining['Description_y'].to_list(),score_cutoff=87))
try:
    ledger_remaining['Description_New'],ledger_remaining['Match_Percent'] = ledger_remaining['matched_description'].str[0],ledger_remaining['matched_description'].str[1]
except Exception:
    pass
ledger_remaining['matched_description']=ledger_remaining['matched_description'].fillna('-')


matched=["no_match"]
ledger_remaining['filter1']=ledger_remaining['matched_description'].map({'-':"no_match"})
probable_match3 = ledger_remaining.loc[~ledger_remaining.filter1.isin(matched)]

ledger_only=ledger_remaining.loc[ledger_remaining.filter1.isin(matched)]


probable_match3['count_desc_y']=probable_match3.groupby('Description_New').cumcount()


probable_match3_new=pd.merge(probable_match3,bs_remaining,left_on=['Description_New','count_desc_y'],right_on=['Description_y','count_y'],how='left')

probable_match3_bs=probable_match3_new.copy()
probable_match3_bs.drop([
                        'Date',
                        'Description',
                        'Amount',
                        'count',
                        'matched_description',
                        'Description_New',
                        'Match_Percent',
                        'filter1',
                        'count_desc_y'
                        ],axis=1,inplace=True)

bs_only=bs_remaining.append(probable_match3_bs).reset_index(drop=True)
bs_only=bs_only.drop_duplicates(keep=False)

merged.rename({
                'Date':'Date_Ledger',
                'Description':'Description_Ledger',
                'Amount':'Amount_Legder',
                'Date_y':'Date_BS',
                'Description_y':'Description_BS',
                'Amount_y':'Amount_BS'
                },axis=1,inplace=True)
probable_match.rename({
                    'Date':'Date_Ledger',
                    'Description':'Description_Ledger',
                    'Amount':'Amount_Legder',
                    'Date_y':'Date_BS',
                    'Description_y':'Description_BS',
                    'Amount_y':'Amount_BS'
                     },axis=1,inplace=True)
probable_match2_new.drop(['matched_description',
                            'Duplicated_x','filter1'
                        ],axis=1,inplace=True)
probable_match2_new.rename({
                    'Date':'Date_Ledger',
                    'Description':'Description_Ledger',
                    'Amount':'Amount_Legder',
                    'Date_y':'Date_BS',
                    'Description_y':'Description_BS',
                    'Amount_y':'Amount_BS'
                     },axis=1,inplace=True)
probable_match3_new.drop(['matched_description',
                        'filter1'
                        ],axis=1, inplace=True)
probable_match2_new.rename({
                    'Date':'Date_Ledger',
                    'Description':'Description_Ledger',
                    'Amount':'Amount_Legder',
                    'Date_y':'Date_BS',
                    'Description_y':'Description_BS',
                    'Amount_y':'Amount_BS'
                     },axis=1,inplace=True)

# Date_Ledger Description_Ledger,
# Amount_Ledger,count,
#  Description_New, Match_Percent,  
# count_desc_y, Date_BS, 
# Description_BS, Amount_BS, count_y

firstLevel=merged
secondLevel=probable_match
thirdLevel=probable_match2_new
manualMatch=probable_match3_new
ledgerOnly=ledger_only
bsOnly=bs_only

conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=ngfwcq3f3\mssqlserver01; DATABASE=Recon_DB;Trusted_Connection=yes")
cursor = conn.cursor()

engine = sqlalchemy.create_engine("mssql+pyodbc://ngfwcq3f3\mssqlserver01/Recon_DB?driver=ODBC Driver 17 for SQL Server")

if cursor.tables(table='manualmatch', tableType='TABLE').fetchone():
    print("exists")
else:
    cursor.execute('''
                    CREATE TABLE manualmatch (
                            Date_Ledger datetime,
                            Description_Ledger varchar(1500),
                            Amount_Ledger float,
                            count bigint,
                            Description_New varchar(1500),
                            Match_Percent float,
                            count_desc_y bigint,
                            Date_BS datetime,
                            Description_BS varchar(1500),
                            Amount_BS float,
                            count_y float
                            )
                    ''')
    conn.commit()
firstLevel.to_sql("firstlevelmatch",engine,if_exists='replace')
secondLevel.to_sql("secondlevelmatch",engine,if_exists='replace')
thirdLevel.to_sql("thirdlevelmatch",engine,if_exists='replace')
manualMatch.to_sql("manualmatch",engine,if_exists='append')
ledgerOnly.to_sql("ledgeronly",engine,if_exists='replace') 
bsOnly.to_sql("bsOnly",engine,if_exists='replace')