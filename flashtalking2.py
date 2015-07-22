# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 13:32:43 2014

@author: ayip
"""

import os, pyodbc, codecs
from ftplib import FTP
from datetime import date, datetime
from zipfile import ZipFile

#Change directory and open completed list
os.chdir('C:\\')
#log = open('log_'+date.today().isoformat()+'.txt','w')
completed_csvs_file = codecs.open('completed_csvs.txt','r',encoding='utf-8')
completed_csvs = []
for line in completed_csvs_file:
    a = line.split('\t')
    completed_csvs.append(a[0].replace('\r\n',''))
print completed_csvs
completed_csvs_file.close()
completed_csvs_file = codecs.open('completed_csvs.txt','a',encoding='utf-8')

#Login to SQL Server
user = ''
pw = ''
#conn_str = 'DRIVER={SQL Server};SERVER=madb;DATABASE=AnalyticsTestDB;UID='+user+';PWD='+pw
conn_str = 'DRIVER={SQL Server};SERVER=madb;DATABASE=AnalyticsTestDB;Trusted_Connection=yes'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
server_cols = ['transactionid','campaign','conversiontype','spotlightname','site','placement',
        'creative','configuration','salesvalue','salestype','u1','u2','u3','u4',
        'u5','u6','u7','metro_code','timetosale','salesdate']

#Get list of uploaded sales dates from server
sqlline = "select distinct salesDate FROM AnalyticsTestDB.dbo.JetBlue_Dynamic"
s = cursor.execute(sqlline)
salesDates = []
for i in s:
    if i[0] is not None:
        sd = i[0].date().isoformat()
        salesDates.append(sd)

#Login to FTP
hostname = 'ftpdatastore.flashtalking.net'
user = ''
pw = ''
ftp = FTP(hostname,user,pw)
try:
    ftp.login()
except:
    pass

#Go to Transaction Folder
ftp.cwd('jetblue')
ftp.cwd('This is JetBlue')
#ftp.cwd('rerun')

#Retrieve CSV list, and if there's a new CSV, download it
csvs = ftp.nlst()
downloaded = []
for c in csvs:
    if len(c)>1 and c.replace('.zip','.csv') not in completed_csvs:
        print "Downloading "+c
        #comment out the next 3 lines if you don't want to redownload
        new_file = open(c,'wb')
        ftp.retrbinary('RETR '+c,new_file.write)
        new_file.close()
        downloaded.append(c)
        #log.write("DOWNLOADED: "+c+'\r\n')

ftp.quit()
ftp.close()

#Parse and upload downloaded files
if len(downloaded)<1:
    print 'no new daily file'
for c in downloaded:
    print "Uploading "+c
    #log.write("UPLOADING: "+c+'\r\n')
    if c.find('.zip')>-1:
        z = ZipFile(c)
        z.extractall()
        z.close()
        c = z.namelist()[0]

    #Write new CSV info to server
    new_csv = codecs.open(c,'r',encoding='utf-8')

    #need to determine the columns available, and match to server
    header_line = new_csv.readline()
    if len(header_line)<2:
        print 'bad daily file'
    print "HEADER LINE: "+header_line
    #log.write("HEADER LINE: "+header_line+'\r\n')
    header_array = header_line.lower().replace('\r\n','').replace('\n','').replace(' ','').split(',')
    header_index = []
    for header in header_array:
        header2 = header
        if header == 'saledate':
            header = 'salesdate'
            hi = header_array.index(header2)
            header_array.pop(hi)
            header_array.insert(hi,header)
        if header == 'timetosale(hours)':
            header = 'timetosale'
            hi = header_array.index(header2)
            header_array.pop(hi)
            header_array.insert(hi,header)
        if header in server_cols:
            header_index.append(header_array.index(header))

    #determine dates in the file
    #determine time format
    country = ''
    flag = 0    # 1 for '/', 2 for '-', 3 is found
    for l in new_csv:
        la = l.replace('\r\n','').replace('\n','').split(',')
        if flag < 3:
            for hi in header_index:
                if header_array[hi] == 'salesdate':
                    if la[hi].find('/')>-1 and flag == 0:
                        da = la[hi].split('/')
                        flag = 1
                    elif la[hi].find('-')>-1 and flag == 0:
                        da = la[hi].split('-')
                        flag = 2
                    elif flag == 1:
                        da2 = la[hi].split('/')
                        if da2[0] == da[0] and da2[1] != da[1]:
                            country = 'US'
                            flag = 13
                            break
                        else:
                            country = 'UK'
                            flag = 13
                            break
                    elif flag == 2:
                        da2 = la[hi].split('-')
                        if da2[1] == da[1]:
                            country = 'US'
                            flag = 23
                            break
                        else:
                            country = 'UK'
                            flag = 23
                            break
        else:
            break
    if country == '':   #single day file
        if flag == 1:
            if da[0] not in ['10','11']:
                country = 'UK'
            elif da[1] not in ['10','11']:
                country = 'US'
            else:   #worse case scenario, just manually check and hardcode filenames
                country = 'US'
            flag = 13
        if flag == 2:
            if da[2] not in ['10','11']:
                country = 'UK'
            elif da[1] not in ['10','11']:
                country = 'US'
            else:   #worse case scenario, just manually check and hardcode filenames
                country = 'US'
            flag = 23
    print 'COUNTRY = '+country
    new_csv.seek(0)
    new_csv.readline()
    fileDates = []
    for l in new_csv:
        la = l.replace('\r\n','').replace('\n','').split(',')
        for hi in header_index:
            if header_array[hi] == 'salesdate' and country == 'US':
                if flag == 13:
                    sd = datetime.strptime(la[hi],"%m/%d/%Y").date().isoformat()
                else:
                    sd = datetime.strptime(la[hi],"%Y-%m-%d").date().isoformat()
                if sd not in fileDates:
                    fileDates.append(sd)
            elif header_array[hi] == 'salesdate' and country == 'UK':
                if flag == 13:
                    sd = datetime.strptime(la[hi],"%d/%m/%Y").date().isoformat()
                else:
                    sd = datetime.strptime(la[hi],"%Y-%d-%m").date().isoformat()
                if sd not in fileDates:
                    fileDates.append(sd)
    print 'FILE DATES: '+', '.join(fileDates)
    new_csv.seek(0)
    new_csv.readline()

    #determine whether or not to drop dates in server
    fileDates.sort()
    for i in range(0,len(fileDates)):
        sd = fileDates[i]
        if sd in salesDates:
            if i < len(fileDates)-1:
                print "DROPPING "+sd
                #log.write('DROPPED: '+sd+'\r\n')
                sqlline = "DELETE FROM AnalyticsTestDB.dbo.JetBlue_Dynamic "
                sqlline += "where salesDate='"+sd+"'"
                cursor.execute(sqlline)
                cursor.commit()
                sqlline = "DELETE FROM AnalyticsProdDB.dbo.JetBlue_Dynamic "
                sqlline += "where salesDate='"+sd+"'"
                cursor.execute(sqlline)
                cursor.commit()
            else:
                fileDates.remove(sd)

    #begin parsing the file
    for l in new_csv:
        print l
        sqlline="insert into [dbo].[JetBlue_Dynamic] "
        la = l.replace('\r\n','').replace('\n','').split(',')
        f = 0
        values = []
        cols = []
        for hi in header_index:
            if header_array[hi] == "u5":    #drop n/a with a default val
                if la[hi].lower().find('n') > -1:
                    values.append("'6/6/1966'")
                    cols.append(header_array[hi])
                else:
                    values.append("'"+la[hi]+"'")
                    cols.append(header_array[hi])
            elif header_array[hi] == "salesvalue":  #convert to numeric
                if la[hi].replace('.','').replace('$','').isdigit():
                    values.append(la[hi].replace('$',''))
                    cols.append(header_array[hi])
                else:
                    values.append('NULL')
                    cols.append(header_array[hi])
            else:   #everything else is a varchar
                if header_array[hi]=='salesdate':
                    cols.append('salesdate')
                    if country == 'US' and flag == 13:
                        sd = datetime.strptime(la[hi],"%m/%d/%Y").date().isoformat()
                        values.append("'"+sd+"'")
                    elif country == 'US' and flag == 23:
                        sd = datetime.strptime(la[hi],"%Y-%m-%d").date().isoformat()
                        values.append("'"+sd+"'")
                    elif country == 'UK' and flag == 13:
                        sd = datetime.strptime(la[hi],"%d/%m/%Y").date().isoformat()
                        values.append("'"+sd+"'")
                    elif country == 'UK' and flag == 23:
                        sd = datetime.strptime(la[hi],"%Y-%d-%m").date().isoformat()
                        values.append("'"+sd+"'")
                    if sd not in fileDates:
                        f = 1
                else:
                    cols.append(header_array[hi])
                    values.append("'"+la[hi]+"'")
        if f == 0:
            sqlline+="("+",".join(cols)+") "
            sqlline+="values ("+",".join(values)+")"
            print "INSERTED: "+",".join(values)
            #log.write("INSERTED: "+",".join(values)+'\r\n')
            cursor.execute(sqlline)
            cursor.commit()
    completed_csvs_file.write(c+'\t'+date.today().isoformat()+'\r\n')
    new_csv.close()
    #os.rename(c,'C:\\Users\\ayip\\Documents\\JetBlue\\flashtalking\\completed\\'+c)

sqlline = "insert into AnalyticsProdDB.dbo.JetBlue_Dynamic "
sqlline += "SELECT * FROM AnalyticsTestDB.dbo.JetBlue_Dynamic as A "
sqlline += "where A.salesDate not in "
sqlline += "(select distinct salesDate from AnalyticsProdDB.dbo.JetBlue_Dynamic)"
cursor.execute(sqlline)
cursor.commit()

conn.close()
completed_csvs_file.close()
#log.write('FINISHED!')
#log.close()
