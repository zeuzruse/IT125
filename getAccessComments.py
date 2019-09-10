# -*- coding: utf-8 -*-
"""
Get AccessComments from Excalibur DimAccess table.
Geocode those with addresses.
"""
import pyodbc, datetime
import pandas as pd
import math, os, geocoder, json, requests
from pandas.io.json import json_normalize
import numpy as np
import concurrent.futures
from multiprocessing import Process

def getUniqueAccessComments():
    '''
    Access wsrb-db-prd-002, Excalibur, DimAcess to get unique AccessComments.
    On 8/27/2019, 1312257 unique comments, exluding obvious no addresses.
    '''
    
    cnxn = pyodbc.connect('Driver={SQL Server};'
                        'Server=wsrb-db-prd-002;'
                        'Database=Excalibur;')
    
    print (datetime.datetime.now(),'    Connection to database made')
    
    script = """
            ;select upper(AccessComments) as AccessComments
            , count(AccessComments) AS count 
             from [Excalibur].[mart].[DimAccess] 
             join [Excalibur].[mart].[FactApplicationAccess]
            	on [Excalibur].[mart].[FactApplicationAccess].AccessKey = [Excalibur].[mart].[DimAccess].AccessKey
             where AccessDateTime >= DATEADD(month, -12, GETDATE())
            	and AccessComments <> 'NO MATCH' 
            	and AccessComments not like '%Password%'
            	and AccessComments not like '%WSRB%'
            	and AccessComments not like '%Search%'
            	and AccessComments not like '%User%'
            	and AccessComments not like '%Protection%'
            	and AccessComments not like '%Loss Cost%'
            	and AccessComments not like '%ErrorMessage%'
            	and AccessComments not like '%XXX%'
            	and AccessComments not like '%Inspection%'
            	and AccessComments not like '%Scored%'
            	and AccessComments not like '%Approval%'
            	and AccessComments not like '%Viewed%'
            	and AccessComments not like '%Error%'
            	and AccessComments not like '%Subscriber%'
            	and AccessComments not like '%visible%'
            	and AccessComments not like '%basemap%'
            	and AccessComments not like '%Automated%'
            	and AccessComments not like '%Authorized%'
            	and AccessComments not like '%distance%'
            	and AccessComments not like '%area%'
            	and AccessComments not like '%P.O. Box%'
            	and AccessComments not like '%PO Box%'
            	and AccessComments not like '%TWP%RGE%'
            	and AccessComments not like '%Risk%'
            	and AccessComments <> ''
            	and AccessComments <> '{}'
            	and AccessComments not like '%miles%'
            	and AccessComments not like '%LOCATION ADDRESS%'
            	and AccessComments not like '%BLK %'
             group by AccessComments 
             order by count desc
          """
    df = pd.read_sql_query(script, cnxn)
        
    # standardizing json 
    df['AccessComments'] = df['AccessComments'].replace({'ZIPCODE':'ZIP'}, regex=True)
    
    print (datetime.datetime.now(),'    Writing to CSV')
    
    df.to_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\new_comments_12months_Agg.csv') 
    
    print (datetime.datetime.now(),'    Finished exporting to csv')    
    return df
    
def getLatLong(filename):
    '''
   
    '''
    print (datetime.datetime.now(),'    Geocoding addresses')
    url = 'http://wa-app-prd-001.wsrb.com/AddressWebServiceInternal/api/address/SearchAddressSingleLine/'
    
    df = pd.read_csv(filename)
    name, ext = os.path.splitext(filename)
    
    folder = r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\results'
    parts = filename.split("\\")
    newpath = os.path.join(folder, parts[-1])
    name, ext = os.path.splitext(newpath)
    
    for i, row in df.iterrows():
        if df.loc[i, 'AccessComments'].startswith('STREETADDRESS'):
            temp = df.loc[i,'AccessComments'].replace("STREETADDRESS=", ' ')
            temp = temp.replace("+", ' ')
            temp = temp.replace("&CITY=", ' ')
            temp = temp.replace("&STATE=", ' ')
            temp = temp.replace("&ZIP=", ' ')
            temp = temp.replace('%25', ' ')
        
        if df.loc[i, 'AccessComments'].startswith('{"STREETADDRESS'):
            temp = json_normalize(json.loads(df.loc[i, 'AccessComments']))
            cols = ['STREETADDRESS', 'CITY', 'STATE', 'ZIP' ]
            temp['singleaddress'] = temp[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis =1 )
            
            temp=temp['singleaddress']
            temp = temp.replace({'#':''}, regex=True)
            temp = temp.replace('0  ', '')
        
        if df.loc[i, 'AccessComments'].startswith('{"ZIP'):
            temp = json_normalize(json.loads(df.loc[i, 'AccessComments']))
            cols = ['STREETADDRESS', 'CITY', 'STATE', 'ZIP' ]
            temp['singleaddress'] = temp[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis =1 )
            
            temp=temp['singleaddress']
            temp = temp.replace({'#':''}, regex=True)

        if df.loc[i, 'AccessComments'].startswith('{"INPUT":'):
            df.loc[i, 'AccessComments']=df.loc[i, 'AccessComments'].replace('\\T', ' ')
            temp = json_normalize(json.loads(df.loc[i, 'AccessComments']))['INPUT'][0]
            
        else:
            pass
        
        payload=" '{}' ".format(temp)
        payload=payload.replace('0    ', ' ')
            
        headers = {    'content-type':'application/json',}
        response = requests.post(url, data=payload, headers=headers)
        response_dict = json.loads(response.text)
        df_temp=json_normalize(response_dict,'AddressGeocoded')
        #print(df_temp.AddressLocation[0]['X'], df_temp.AddressLocation[0]['Y'])
        
        # handle when return is empty
        if df_temp.empty or df_temp['AddressLocation'].empty:
            pass
        else:
            df.at[i, 'lat'] = df_temp.AddressLocation[0]['Y']
            df.at[i, 'long'] = df_temp.AddressLocation[0]['X']
            df.at[i, 'confidence'] = df_temp.Confidence[0]
            df.at[i, 'source'] = df_temp.Source[0]
    
    df.to_csv(name + '_latlong' + ext, header=True, index=False, float_format="%.6f")
    print (datetime.datetime.now(),'    Finished geocoding')
    return df

def joinFrames(df_right):
    
    df_left = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\lastday_AccessComments.csv')
    df_join = pd.concat([df_left, df_right], sort=True)
    df_join.to_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\lastday_AccessComments_latlong.csv')

def main():
    t1 = datetime.datetime.now()
    print (t1)
    
    df = getUniqueAccessComments()
    
    # hard coding for now so don't need to send query
    #with concurrent.futures.ProcessPoolExecutor() as executor:
    
    #df = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\test_multiprocessing.csv')
    
    #df = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\split_files_7841\new_comments_12months_Agg{.csv')
    a = r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\split_files\new_comments_12months_Agg_2.csv'
    b = r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\split_files\new_comments_12months_Agg_3.csv'
    #c = r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\split_files\new_comments_12months_Agg_4.csv'
    #d = r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\split_files\new_comments_12months_Agg_5.csv'
    
    Process(target=getLatLong, args=a).start()
    Process(target=getLatLong, args=b).start()    
    
#    p1=Process(target = getLatLong(df))
#    p1.start()
#    p2=Process(target = getLatLong(df))
#    p2.start()
#    p1.join()
#    p2.join()
    #df = getLatLong(df)
    
    #joinFrames(df)
    
    t2 = datetime.datetime.now()
    print (t2 - t1)

if __name__ == '__main__':
    main()
