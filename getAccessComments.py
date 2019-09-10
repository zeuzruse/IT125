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

def getUniqueAccessComments():
    '''
    Access wsrb-db-prd-002, Excalibur, DimAcess to get unique AccessComments.
    On 8/27/2019, 1,312,257 unique comments, exluding obvious no addresses.
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
    # print (df.info())
    
    # DOUBLE CHECK DROPPED COLUMNS
    # drop 'Password Reset Ind', 'Rec Eff End Date', 'Rec Delete Date', 
    #       'Rec Load Date Time', 'Rec Update Date Time'
    
    # not necessary, leave out of SQL query
#    df = df.drop([ 'Rec Delete Date', 'Rec Load Date Time', 'Rec Update Date Time'], axis = 1)
    
    # not found
    #df = df.drop([ 'Unnamed: 0'], axis = 1)
    
    # handle in SQL query
    #df['AccessComments'] = df['AccessComments'].str.upper()
    
    # standardizing json so when normalized there isn't extra columns
    df['AccessComments'] = df['AccessComments'].replace({'ZIPCODE':'ZIP'}, regex=True)
    
    # random sample
    #df_random = df.sample(n=20)
    # df_random[:10]
    
    print (datetime.datetime.now(),'    Writing to CSV')
    
    df.to_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\new_comments_12months_Agg.csv')
    
#    writer = pd.ExcelWriter(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\new_comments.xlsx')
#    df.to_excel(writer, sheet_name='new_comments')    
#    writer.save()    
    
    print (datetime.datetime.now(),'    Finished exporting to csv')    
    return df
    
def getLatLong(df):
    '''
    need to add try except 
    '''
    print (datetime.datetime.now(),'    Geocoding addresses')
    url = 'http://wa-app-prd-001.wsrb.com/AddressWebServiceInternal/api/address/SearchAddressSingleLine/'

    for i, row in df.iterrows():
        if df.loc[i, 'AccessComments'].startswith('STREETADDRESS'):
            temp = df.loc[i,'AccessComments'].replace("STREETADDRESS=", ' ')
            temp = temp.replace("+", ' ')
            temp = temp.replace("&CITY=", ' ')
            temp = temp.replace("&STATE=", ' ')
            temp = temp.replace("&ZIP=", ' ')
            temp = temp.replace('%25', ' ')
        elif df.loc[i, 'AccessComments'].startswith('{"STREETADDRESS'):
            temp = json_normalize(json.loads(df.loc[i, 'AccessComments']))
            cols = ['STREETADDRESS', 'CITY', 'STATE', 'ZIP' ]
            temp['singleaddress'] = temp[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis =1 )
            temp=temp['singleaddress']
            temp = temp.replace({'#':''}, regex=True)
            temp = temp.replace('0  ', '')
        elif df.loc[i, 'AccessComments'].startswith('{"ZIP'):
            temp = json_normalize(json.loads(df.loc[i, 'AccessComments']))
            cols = ['STREETADDRESS', 'CITY', 'STATE', 'ZIP' ]
            temp['singleaddress'] = temp[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis =1 )
            temp=temp['singleaddress']
            temp = temp.replace({'#':''}, regex=True)
        elif df.loc[i, 'AccessComments'].startswith('{"INPUT":'):
            df.loc[i, 'AccessComments']=df.loc[i, 'AccessComments'].replace('\\T', ' ')
            temp = json_normalize(json.loads(df.loc[i, 'AccessComments']))['INPUT'][0]
        else:
            pass
        
        payload=" '{}' ".format(temp)
        payload=payload.replace('0    ', ' ')
        payload=payload.split('Name')[0]+"'"
        headers = {    'content-type':'application/json',}
        response = requests.post(url, data=payload, headers=headers)
        response_dict = json.loads(response.text)
        df_temp=json_normalize(response_dict,'AddressGeocoded')
        
        if df_temp.empty or df_temp['AddressLocation'].empty:
            pass
        else:
            df.at[i, 'lat'] = df_temp.AddressLocation[0]['Y']
            df.at[i, 'long'] = df_temp.AddressLocation[0]['X']
            df.at[i, 'confidence'] = df_temp.Confidence[0]
            df.at[i, 'source'] = df_temp.Source[0]
            df.at[i, 'inputAddress'] = df_temp.InputAddress

            #df.at[i, 'singlelineaddress'] = temp
    
    df.to_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\results\new_comments_12months_Agg{_latlong.csv')
    print (datetime.datetime.now(),'    Finished geocoding')
    return df

def joinFrames(df_right):
    
    df_left = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\lastday_AccessComments.csv')
    
    df_join = pd.concat([df_left, df_right], sort=True)
    
    df_join.to_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\lastday_AccessComments_latlong.csv')

def main():
    # print date time before processing
    t1 = datetime.datetime.now()
    print (t1)
    
    #df = getUniqueAccessComments()
    
    # return variable from getAccessComments()
    # raw = r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\new_comments.csv'
    
    # hard coding for now so don't need to send query
    #with concurrent.futures.ProcessPoolExecutor() as executor:
    
    #df = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\test_multiprocessing.csv')
    
    df = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\split_files_499\new_comments_12months_Agg{.csv')
    
#    p1=Process(target = getLatLong(df))
#    p1.start()
#    p2=Process(target = getLatLong(df))
#    p2.start()
#    p1.join()
#    p2.join()
    df = getLatLong(df)
    
    
    #joinFrames(df)
    
    # print date time after processing
    t2 = datetime.datetime.now()
    #find total time elapsed
    print (t2 - t1)

if __name__ == '__main__':
    main()