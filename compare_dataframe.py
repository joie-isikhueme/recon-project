from collections import OrderedDict as od
import pandas as pd
import fuzzywuzzy
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import pyodbc
import sqlalchemy
df1 = pd.read_excel(r"C:\Users\jisikhueme003\Documents\Recon\Zenith Ledger 1-31.01.2022.xlsx",sheet_name='Sheet1')

d1 = df1[["Date","Description","Amount"]]

df2 = pd.read_excel(r"C:\Users\jisikhueme003\Documents\Recon\Zenith Bank Statement - 1-31.01.22.xlsx", sheet_name='Zenith Statement')

df2["Credit Amount"] = df2["Credit Amount"].fillna(0)

df2["Debit Amount"] = df2["Debit Amount"].fillna(0)

df2["Amount"] = df2["Credit Amount"] + df2["Debit Amount"]

d2 = df2[["Effective Date","Description","Amount"]]


d2.columns=["Date","Description","Amount"]

def diff_func(df_left, df_right, uid, labels=('Left', 'Right'), drop=[[],[]]):
    dict_df = {labels[0]: df_left, labels[1]: df_right}
    col_left = df_left.columns.tolist()
    col_right = df_right.columns.tolist()

    # There could be columns known to be different, hence allow user to pass this as a list to be dropped.
    if drop[0] or drop[1]:
        print ('{}: Ignoring columns {} in comparison.'.format(labels[0], ', '.join(drop[0])))
        print ('{}: Ignoring columns {} in comparison.'.format(labels[1], ', '.join(drop[1])))
        col_left = list(filter(lambda x: x not in drop[0], col_left))
        col_right = list(filter(lambda x: x not in drop[1], col_right))
        df_left = df_left[col_left]
        df_right = df_right[col_right]

    # Step 1 - Check if no. of columns are the same:
    len_lr = len(col_left), len(col_right)
    assert len_lr[0]==len_lr[1], \
    'Cannot compare frames with different number of columns: {}.'.format(len_lr)

    # Step 2a - Check if the set of column headers are the same
    #           (order doesnt matter)
    assert set(col_left)==set(col_right), \
    'Left column headers are different from right column headers.' \
       +'\n   Left orphans: {}'.format(list(set(col_left)-set(col_right))) \
       +'\n   Right orphans: {}'.format(list(set(col_right)-set(col_left)))

    # Step 2b - Check if the column headers are in the same order
    if col_left != col_right:
        print ('[Note] Reordering right Dataframe...')
        df_right = df_right[col_left]

    # Step 3 - Check datatype are the same [Order is important]
    if all(df_left.dtypes == df_right.dtypes):
        print ('DataType check: Passed')
    else:
        print ('dtypes are not the same.')
        df_dtypes = pd.DataFrame({labels[0]:df_left.dtypes,labels[1]:df_right.dtypes,'Diff':(df_left.dtypes == df_right.dtypes)})
        df_dtypes = df_dtypes[df_dtypes['Diff']==False][[labels[0],labels[1],'Diff']]
        print (df_dtypes)

    # Step 4 - Check for duplicate rows
    for key, df in dict_df.items():
        if df.shape[0] != df.drop_duplicates().shape[0]:
            print(key + ': Duplicates exists, they will be dropped.')
            dict_df[key] = df.drop_duplicates()

    # Step 5 - Check for duplicate uids.
    if isinstance(uid, (str, list)):
        print ('Uniqueness check: {}'.format(uid))
        for key, df in dict_df.items():
            count_uid = df.shape[0]
            count_uid_unique = df[uid].drop_duplicates().shape[0]
            dp = [0,1][count_uid_unique == df.shape[0]] #<-- Round off to the nearest integer if it is 100%
            pct = round(100*count_uid_unique/df.shape[0], dp)
            print ('{}: {} out of {} are unique ({}%).'.format(key, count_uid_unique, count_uid, pct))

    # Checks complete, begin merge. 
    d_result = od()
    d_result[labels[0]] = df_left
    d_result[labels[1]] = df_right
    if all(df_left.eq(df_right).all()):
        print('Trival case: DataFrames are an exact match.')
        d_result['Merge'] = df_left.copy()
    else:
        df_merge = pd.merge(df_left, df_right, on=col_left, how='inner')
        if not df_merge.shape[0]:
            print('Trival case: Merged DataFrame is empty')
        
        d_result['Merge'] = df_merge
        if type(uid)==str:
            uid = [uid]

        if type(uid)==list:
            df_left_only = df_left.append(df_merge).reset_index(drop=True)
            df_left_only['Duplicated']=df_left_only.duplicated(keep=False)  #keep=False, marks all duplicates as True
            df_left_only = df_left_only[~df_left_only['Duplicated']]
            df_right_only = df_right.append(df_merge).reset_index(drop=True)
            df_right_only['Duplicated']=df_right_only.duplicated(keep=False)
            df_right_only = df_right_only[~df_right_only['Duplicated']]

            label = '{} or {}'.format(*labels)
            df_lc = df_left_only.copy()
            df_lc[label] = labels[0]
            df_rc = df_right_only.copy()
            df_rc[label] = labels[1]
            df_c = df_lc.append(df_rc).reset_index(drop=True)
            df_c['Duplicated'] = df_c.duplicated(subset=uid, keep=False)
            df_c1 = df_c[df_c['Duplicated']]
            df_c1 = df_c1.drop('Duplicated', axis=1)
            cols = df_c1.columns.tolist()
            df_c1 = df_c1[[cols[-1]]+cols[:-1]]
            df_uc = df_c[~df_c['Duplicated']]

            df_uc_left = df_uc[df_uc[label]==labels[0]]
            df_uc_right = df_uc[df_uc[label]==labels[1]]

            d_result[labels[0]+'_only'] = df_uc_left.drop(['Duplicated', label], axis=1)
            d_result[labels[1]+'_only'] = df_uc_right.drop(['Duplicated', label], axis=1)
            d_result['Diff'] = df_c1.sort_values(uid).reset_index(drop=True)
    
    return d_result

d_1 = diff_func(d1, d2, 'Amount')

d_2 = diff_func(d1, d2, 'Description')

diff_ = d_1["Diff"]

merge= d_1['Merge']

leftOnly = d_1['Left_only']

rightOnly = d_1['Right_only']

probable = diff_

probable["code"] = probable["Description"] + probable["Amount"].astype(str)

prob_l = probable.loc[probable["Left or Right"] == "Left"]

prob_r = probable.loc[probable["Left or Right"] == "Right"]

prob_result_ = pd.merge(prob_l, prob_r, on=["code"],how='inner')


ccc=prob_result_['code']
probable_2=probable[~probable.code.isin(ccc)]


prob2_l = probable_2.loc[probable_2["Left or Right"] == "Left"]

prob2_r = probable_2.loc[probable_2["Left or Right"] == "Right"]



prob2_l['matched_description']=prob2_l['Description'].apply(lambda x: process.extractOne(x, prob2_r['Description'].to_list(),score_cutoff=87))
try:
    prob2_l['Description_New'],prob2_l['Match_Percent'] = prob2_l['matched_description'].str[0],prob2_l['matched_description'].str[1]
except Exception:
    pass

prob2_l['matched_description']=prob2_l['matched_description'].fillna('-')




matched=["no_match"]
prob2_l['filter1']=prob2_l['matched_description'].map({'-':"no_match"})
prob2_result_ = prob2_l.loc[~prob2_l.filter1.isin(matched)]

l=[]
l.extend(prob2_result_['Description'].tolist())
l.extend(prob2_result_['Description_New'].tolist())
l=pd.DataFrame(l).fillna('-')
l.columns=['value']

a=['-']
l=l.loc[~l.value.isin(a)]

ccc=l["value"]

probable_3=probable_2[~probable_2.Description.isin(ccc)]

prob3_l = probable_3.loc[probable_3["Left or Right"] == "Left"]

prob3_r = probable_3.loc[probable_3["Left or Right"] == "Right"]


prob3_l['matched_description']=prob3_l['code'].apply(lambda x: process.extractOne(x, prob3_r['code'].to_list(),score_cutoff=80))

try:
    prob3_l['Description_New'],prob3_l['Match_Percent'] = prob3_l['matched_description'].str[0],prob3_l['matched_description'].str[1]
except Exception:
    pass

firstLevel = merge
secondLevel = prob_result_
thirdLevel = prob2_result_
manualMatch = prob3_l

manualMatch=manualMatch.drop(
                            ['Left or Right',
                             'matched_description',
                             'code','Match_Percent']
                            ,axis=1)
manualMatch['Description_Statement']=manualMatch['Description_New']

thirdLevel=thirdLevel.drop([
                            'Left or Right','matched_description',
                            'code','Match_Percent','filter1']
                            ,axis=1
                            )
thirdLevel['Description_New']=thirdLevel['Description_Statement']

secondLevel=secondLevel.drop(['Left or Right_x','Left or Right_y','code'],axis=1)

conn = pyodbc.connect("""
                      DRIVER={ODBC Driver 17 for SQL Server}; 
                      SERVER=ngfwcq3f3\mssqlserver01; 
                      DATABASE=NameTestDB;Trusted_Connection=yes
                      """)
cursor = conn.cursor()

engine = sqlalchemy.create_engine(
                    """
                    mssql+pyodbc://ngfwcq3f3\mssqlserver01/NameTestDB?
                    driver=ODBC Driver 17 for SQL Server
                    """)

firstLevel.to_sql("firstlevelmatch",engine,if_exists='replace')
secondLevel.to_sql("secondlevelmatch",engine,if_exists='replace')
thirdLevel.to_sql("thirdlevelmatch",engine,if_exists='replace')
manualMatch.to_sql("manualmatch",engine,if_exists='replace')







#prob_l['matched_description']=prob_l['Description'].apply(lambda x: process.extractOne(x, prob_r['Description'].to_list(),score_cutoff=87))

#try:
#    prob_l['Description_New'],prob_l['Match_Percent'] = prob_l['matched_description'].str[0],prob_l['matched_description'].str[1]
#except Exception:
#    pass



#k=[None]

#cck=prob_l[~prob_l.Description_New.isin(k)]


