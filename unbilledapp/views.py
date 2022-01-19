# Importing all nessosary libraries
from django.shortcuts import render, HttpResponse
from django.shortcuts import redirect
from django.contrib.auth.models import User, auth
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
import statistics 
import json
import pandas as pd
from django.core.mail import EmailMultiAlternatives
from django.conf import settings
from django.core.mail import send_mail
import os
import glob
import schedule
import time
import xlrd
import pyodbc
from django.contrib.auth import get_user_model
from datetime import datetime
from django.core.files.storage import FileSystemStorage
import datetime
import dateutil.relativedelta
from datetime import datetime, timedelta, date

pd.options.display.float_format = "{:,.0f}".format
# Create your views here.
conn = pyodbc.connect('Driver={SQL Server};''Server=192.168.0.117;''Database=AJWorldWide;''UID=ajview;''PWD=aj$%^World@123;''Trusted_Connection=no;')

def index(request):
    er_msg = ''
    if request.method == 'POST':
        username = request.POST['email'] #username
        password = request.POST['password'] #password
        user = authenticate(username=username, password=password)  # Authendicating user
        if user is not None:
            login(request,user)  # if user availlable login
            if request.user.username == 'support@ajww.com': # if support meand redirect to support dashboard
                return redirect('/dashboard/')
            else:
                print("error1")
        else:
            print("error2")
            er_msg = 'True'
    context={'er_msg':er_msg}
    return render(request,'index.html',context)





def logout_view(request):  # Logout and redirect to login page
    logout(request)
    return redirect('/')


@login_required
def dashboard(request):
    msg_file = ''
    if request.method == 'POST':
        msg_file = 'True'
        file_name = request.FILES['upload_file']
        with open('data_sheet.xlsx', 'wb+') as destination:  
            for chunk in file_name.chunks():  
                destination.write(chunk)
            msg_file = 'True'
    context = {'msg_file':msg_file}
    return render(request,'dashboard.html',context)

def date_fill(c):
    if c["Job Dept"] == "FIS":
        if str(c['Actual Delivery Date']) != "NaT":
            return c['Actual Delivery Date']
        
        elif str(c['Estimated Delivery Date']) != "NaT":
            print("PASSS SECOND CONDITION")
            return c['Estimated Delivery Date']
        
        elif c['Direction'] == 'Import':
            if c['Destination ETA'] == "":
                return c['Job Opened']
            else:
                return c['Destination ETA']
            
        elif c['Direction'] == 'Export':
            if c['Origin ETD'] == "":
                return c['Job Opened']
            else:
                return c['Origin ETD']

        elif c['Direction'] == '':
            return c['Job Opened']
        else:
            if c['Destination ETA'] == "":
                return c['Job Opened']
            else:
                return c['Destination ETA']
    else:
        if c['Direction'] == 'Import':
            if c['Destination ETA'] == "":
                return c['Job Opened']
            else:
                return c['Destination ETA']
            
        elif c['Direction'] == 'Export':
            if c['Origin ETD'] == "":
                return c['Job Opened']
            else:
                return c['Origin ETD']

        elif c['Direction'] == '':
            return c['Job Opened']
        else:
            if c['Destination ETA'] == "":
                return c['Job Opened']
            else:
                return c['Destination ETA']


@login_required
def mail_data(request):
    # Reading raw data 
    df = pd.read_excel('data_sheet.xlsx',skiprows =14)
    # Deleting Unwanted Columns 
    del df['Unnamed: 0']
    del df['Unnamed: 1']

    # for unbilled job Status must ["WRK","WHL","IHL"]
    job_status_filter_names = ["WRK","WHL","IHL"]

    # Creating New Dataframe from raw data for unbilled - df_filtered
    df_filtered = df[df["Job Status"].isin(job_status_filter_names)]
    df_filtered = df_filtered[['Shipment ID',"Origin","Destination", 'Estimated Delivery Date', 'Actual Delivery Date', 'Mode','Direction' ,'Origin ETD','Destination ETA','Job Opened','Controlling Customer Name','Job Status','Job Sales Rep','Job Operator','Job Dept','Origin','Destination','Mode','Trans','Incoterm']]
    df_filtered['Date'] = df_filtered.apply(date_fill, axis=1)

    # Reading Operator Details From Database
    operator = pd.read_sql('select * from [dbo].[dimsalesman]',conn)
    operator.rename(columns={'Job Operator':'Job Operator Code'}, inplace=True)
    operator.rename(columns={'FullName':'Job Operator'}, inplace=True)

    # Merging filterd data and operator data based on ["Job Operator"] column and Storing Merged data into df_full
    df_full = pd.merge(df_filtered, operator, on="Job Operator", how='left')
    df_full.columns = ['Shipment ID',"Origin","Destination", 'Estimated Delivery Date', 'Actual Delivery Date', 'Mode','Direction' , 'ETD','ETA','Job Opened','Controlling Customer','Job Status','Sales Rep','Job Operator','Dept','Origin','Dest.','Cont.','Trans.','Incoterm','Date','Loginname','Job Operator Code','Country','EmailAddress','PBIEmail']
    df_full['FullName'] = df_full['Job Operator']

    # Making copy of Merged data into df_with_email
    df_with_email = df_full
    d = datetime.today() - timedelta(days=3)

    df_full = df_full[df_full['Date'] <= d ]
    import datetime
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    dat = lastMonth.strftime("%m")
    dat = int(dat)
    print(dat)
    todays_date = date.today()
    year_cuurent = todays_date.year

    # Creating month , year  Columns from Date column and converting those datatypes
    df_full['month'] = pd.DatetimeIndex(df_full['Date']).month
    df_full['year'] = pd.DatetimeIndex(df_full['Date']).year
    df_full['month'] = df_full['month'].astype('int32')
    df_full['year']  = df_full['year'].astype('int32')

    # Exporting the Filterd Unbilled Raw Data
    df_full.to_excel('Filterd Unbilled Raw Data.xlsx')

    # Creating Department list
    department_list = ["FDR","FIA","FIS","FES","FEA"]

    import calendar
    for dpt_name in department_list:
        data_table_all = """
            <head>
            <style type='text/css'>
            table {
            border-collapse: collapse;
            }
            th {
            text-align: left !important;
            padding: 2px;
            }
            td {
            text-align: center !important;
            padding: 2px;
            }
            tr:nth-child(even){background-color: #f2f2f2}
            th {
            background-color: #1b30ab;
            color: #fff;
            }
            </style>
            </head>
            <p><b>Hi Team</b>  <br/><br/> Please find the below unbilled shipments Kindly expedite the billing process <br/> <br/>Current Date minus three days (ETD  Exports and ETA  Imports ) has been  considered as Unbilled <br/>  <br/>Best Regards <br/>IT Control Tower <br/> <br/>This is an automated unbilled email tigger for any queries on this please reach out kalirajan@ajww.com or sabith@ajww.com or accounts4@ajwwbo.com <br/>"""
        data_table_all  = data_table_all + " <h2>Department of " + dpt_name +" </h2> <br/>"

        # creating new filterd  dataframe from department name
        df_val_count = df_full[df_full["Dept"]==dpt_name]
        df_val_count = df_val_count[["FullName","month",'year']].value_counts()
        df_value_counts = df_val_count.reset_index()
        df_value_counts.columns = ['FullName','Month',"year","ShipmentCount"]

        df_value_counts["Month Name"] = df_value_counts["Month"]

        df_value_counts = df_value_counts.sort_values(['year','Month'],ascending=[True, True])

        df_value_counts['Month Name'] = df_value_counts['Month Name'].apply(lambda x: str(x)+"."+calendar.month_abbr[x])

        cols = ['Month Name', 'year']

        df_value_counts['Month Name'] = df_value_counts[cols].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

        # creating pivot_table from operator fullname as index and ['year',"Month","Month Name"] as columns and ShipmentCount as values

        table = pd.pivot_table(df_value_counts,index=['FullName',],columns=['year',"Month","Month Name"],values=['ShipmentCount'])
        table = table.fillna(0)

        table.index.name = None

        col_name = []
        for col_name1 in table.columns:
            col_name.append(col_name1[3])
        table.columns =col_name

        table["Total"] = table.sum(axis=1)
        table.loc['Total']= table.sum()
        table = table.replace(0,'')
        table["Sales Person"] = table.index
        table.insert(0,'Operators',table["Sales Person"])
        del table['Sales Person']
        table = table.reset_index(drop=True)

        
        df_sales_person_email = list(df_with_email[df_with_email['Dept']==dpt_name]['EmailAddress'].unique())

        data_table_all = data_table_all + table.to_html(index=False)
        data_table_all = data_table_all + "<br/><br/><br/>"
        data_table_all=data_table_all.replace('\n','')
        table_str = table.to_string()
        table_str = table_str.replace('\n','').strip()

        # Replacing all unwanted data inside dataframe
        data_table_all = str(data_table_all)
        data_table_all = data_table_all.replace('11.','')
        data_table_all = data_table_all.replace('12.','')
        data_table_all = data_table_all.replace('1.','')
        data_table_all = data_table_all.replace('2.','')
        data_table_all = data_table_all.replace('3.','')
        data_table_all = data_table_all.replace('4.','')
        data_table_all = data_table_all.replace('5.','')
        data_table_all = data_table_all.replace('6.','')
        data_table_all = data_table_all.replace('7.','')
        data_table_all = data_table_all.replace('8.','')
        data_table_all = data_table_all.replace('9.','')
        data_table_all = data_table_all.replace('10.','')

        # Creating Raw data for itrated Department
        file_name = dpt_name + "_Department.xlsx"
        df_mail = df_full[df_full["Dept"]==dpt_name]
        df_mail = df_mail[["Shipment ID","Origin","Destination",'ETD','ETA','Date','Mode',"Dept","Job Operator","Controlling Customer","Job Status"]]
        df_mail = df_mail.sort_values("Job Operator", axis = 0, ascending = True,)
        df_mail.to_excel(file_name,index=False)

        # Triggering Unbilled Mail based on department
        from datetime import datetime
        today_date_sub = datetime.today().strftime('%d-%m-%Y')
        subject = "Unbilled Trigger - "+str(dpt_name) + "-" + str(today_date_sub)
        from_email = settings.EMAIL_HOST_USER

        to = df_sales_person_email
        if dpt_name == "FIA":
            cc = ['kalirajan@ajww.com','sabith@ajww.com','support@ajwwbo.com','creditcontrol2@ajww.com']
        elif dpt_name == "FIS":
            cc = ['kalirajan@ajww.com','sabith@ajww.com','support@ajwwbo.com','creditcontrol2@ajww.com']
        elif dpt_name == "FDR":
            cc = ['kalirajan@ajww.com','sabith@ajww.com','support@ajwwbo.com','creditcontrol2@ajww.com']

        elif dpt_name == "FEA":
            cc = ['kalirajan@ajww.com','sabith@ajww.com','support@ajwwbo.com','creditcontrol2@ajww.com']

        elif dpt_name == "FES":
            cc = ['kalirajan@ajww.com','sabith@ajww.com','support@ajwwbo.com','creditcontrol2@ajww.com']

        elif dpt_name == "FEA":
            cc = ['kalirajan@ajww.com','sabith@ajww.com','support@ajwwbo.com','creditcontrol2@ajww.com']
        
        else:
            cc=[]

        data_table_all = data_table_all + "</center></body>"
        msg = EmailMultiAlternatives(subject, table_str, from_email, to, cc=cc)
        msg.attach_alternative(data_table_all, "text/html")
        file_name = dpt_name + "_Department.xlsx"
        msg.attach_file(file_name)
        msg.send()
        
        # Making another trigger based on Departmnet for different User

        if dpt_name == "FDR":
            to=["danny@ajww.com","sathish@ajww.com","rick@ajww.com"]
            cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']
            
        elif dpt_name == "FIS":
            to=["danny@ajww.com","sathish@ajww.com","rick@ajww.com"]
            cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']               

        elif dpt_name == "FIA":
            to=["cecil@ajww.com","vicky@ajww.com"]
            cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']  
            
        elif dpt_name == "FES":
            to=["sinem@ajww.com","vivien@ajww.com","onur@ajww.com"]
            cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']   
            
        else:
            pass
	
        if dpt_name != "FEA":
            msg = EmailMultiAlternatives(subject, table_str, from_email, to, cc=cc)
            msg.attach_alternative(data_table_all, "text/html")
            file_name = dpt_name + "_Department.xlsx"
            msg.attach_file(file_name)
            msg.send()


        # Creating the same process for FEA Department with different preprocesing ( Spliting mails for india and turky )
        if dpt_name == "FEA":
            df_filtered = df[df["Job Status"].isin(job_status_filter_names)]
            df_filtered = df_filtered[['Shipment ID','Destination Country', "Origin","Destination",'Estimated Delivery Date', 'Actual Delivery Date', 'Mode','Direction' ,'Origin ETD','Destination ETA','Job Opened','Controlling Customer Name','Job Status','Job Sales Rep','Job Operator','Job Dept','Origin','Destination','Mode','Trans']]
            df_filtered['Date'] = df_filtered.apply(date_fill, axis=1)
            operator = pd.read_sql('select * from [dbo].[dimsalesman]',conn)
            operator.rename(columns={'Job Operator':'Job Operator Code'}, inplace=True)
            operator.rename(columns={'FullName':'Job Operator'}, inplace=True)
            df_full = pd.merge(df_filtered, operator, on="Job Operator", how='left')
            print(df_full)
            df_full.columns = ['Shipment ID','Destination Country', "Origin","Destination",'Estimated Delivery Date', 'Actual Delivery Date', 'Mode','Direction' , 'ETD','ETA','Job Opened','Controlling Customer','Job Status','Sales Rep','Job Operator','Dept','Origin','Dest.','Cont.','Trans.','Date','Loginname','Job Operator Code','Country','EmailAddress','PBIEmail']
            df_full['FullName'] = df_full['Job Operator']
            df_with_email = df_full
            from datetime import datetime, timedelta, date
            d = datetime.today() - timedelta(days=3)
            df_full = df_full[df_full['Date'] <= d ]
            import datetime
            today = datetime.date.today()
            first = today.replace(day=1)
            lastMonth = first - datetime.timedelta(days=1)
            dat = lastMonth.strftime("%m")
            dat = int(dat)
            print(dat)
            todays_date = date.today()
            year_cuurent = todays_date.year

            df_full['month'] = pd.DatetimeIndex(df_full['Date']).month
            df_full['year'] = pd.DatetimeIndex(df_full['Date']).year

            df_full['month'] = df_full['month'].astype('int32')
            df_full['year']  = df_full['year'].astype('int32')

            df_full.to_excel('Filterd Unbilled Raw Data.xlsx')

            df_val_count1 = df_full[df_full["Dept"]=="FEA"]
            destination = ["IN","TR","Rest of World"]

            import calendar
            for dpt_name in destination:
                data_table_all = """
                    <head>
                    <style type='text/css'>
                    table {
                    border-collapse: collapse;
                    }
                    th {
                    text-align: left !important;
                    padding: 2px;
                    }
                    td {
                    text-align: center !important;
                    padding: 2px;
                    }
                    tr:nth-child(even){background-color: #f2f2f2}
                    th {
                    background-color: #1b30ab;
                    color: #fff;
                    }
                    </style>
                    </head>
                    <p><b>Hi Team</b>  <br/><br/> Please find the below unbilled shipments Kindly expedite the billing process <br/> <br/>Current Date minus three days (ETD  Exports and ETA  Imports ) has been  considered as Unbilled <br/>  <br/>Best Regards <br/>IT Control Tower <br/> <br/>This is an automated unbilled email tigger for any queries on this please reach out kalirajan@ajww.com or sabith@ajww.com or accounts4@ajwwbo.com <br/>"""
                data_table_all  = data_table_all + " <h2>Department of FEA - Country : " + dpt_name +" </h2> <br/>"
                
                if dpt_name == "IN":
                    df_val_count = df_val_count1[df_val_count1["Destination Country"]=="IN"]
                elif dpt_name == "TR":
                    df_val_count = df_val_count1[df_val_count1["Destination Country"]=="TR"]
                else:
                    df_val_count = df_val_count1[(df_val_count1["Destination Country"]!="TR")&(df_val_count1["Destination Country"]!="IN")]

                df_val_count = df_val_count[["FullName","month",'year']].value_counts()
                df_value_counts = df_val_count.reset_index()
                df_value_counts.columns = ['FullName','Month',"year","ShipmentCount"]

                # df_val_count = df_val_count[df_val_count["Destination Country"]==dpt_name]

                df_value_counts["Month Name"] = df_value_counts["Month"]

                df_value_counts = df_value_counts.sort_values(['year','Month'],ascending=[True, True])

                df_value_counts['Month Name'] = df_value_counts['Month Name'].apply(lambda x: str(x)+"."+calendar.month_abbr[x])

                cols = ['Month Name', 'year']

                df_value_counts['Month Name'] = df_value_counts[cols].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

                table = pd.pivot_table(df_value_counts,index=['FullName',],columns=['year',"Month","Month Name"],values=['ShipmentCount'])
                table = table.fillna(0)

                table.index.name = None

                col_name = []
                for col_name1 in table.columns:
                    col_name.append(col_name1[3])
                table.columns =col_name

                table["Total"] = table.sum(axis=1)
                table.loc['Total']= table.sum()
                table = table.replace(0,'')
                table["Sales Person"] = table.index
                table.insert(0,'Operators',table["Sales Person"])
                del table['Sales Person']
                table = table.reset_index(drop=True)

                print(table)

                # data_table_all = data_table_all + str(df_sales_person_email)

                data_table_all = data_table_all + table.to_html(index=False)
                data_table_all = data_table_all + "<br/><br/><br/>"
                data_table_all=data_table_all.replace('\n','')
                table_str = table.to_string()
                table_str = table_str.replace('\n','').strip()
                # data_table_all = data_table_all + data_tbl
                data_table_all = str(data_table_all)
                data_table_all = data_table_all.replace('11.','')
                data_table_all = data_table_all.replace('12.','')
                data_table_all = data_table_all.replace('1.','')
                data_table_all = data_table_all.replace('2.','')
                data_table_all = data_table_all.replace('3.','')
                data_table_all = data_table_all.replace('4.','')
                data_table_all = data_table_all.replace('5.','')
                data_table_all = data_table_all.replace('6.','')
                data_table_all = data_table_all.replace('7.','')
                data_table_all = data_table_all.replace('8.','')
                data_table_all = data_table_all.replace('9.','')
                data_table_all = data_table_all.replace('10.','')
                print(data_table_all)

                from datetime import datetime
                today_date_sub = datetime.today().strftime('%d-%m-%Y')
                subject = "Unbilled Trigger FEA Department - Country "+str(dpt_name) + "-" + str(today_date_sub)
                from_email = settings.EMAIL_HOST_USER

                file_name = "FEA_"+dpt_name+"_Department.xlsx"
                df_mail = df_full[df_full["Dept"]=="FEA"]
                
                if dpt_name == "IN":
                    to=["shipaj@ajww.com","metin@ajww.com"]
                    cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']  
                    df_mail = df_mail[df_mail["Destination Country"]==dpt_name]

                elif dpt_name == "TR":
                    to=["filiz@ajww.com","arda@ajww.com"]
                    cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']
                    df_mail = df_mail[df_mail["Destination Country"]==dpt_name]
                else:
                    to=["sarath@ajww.com","steve@ajww.com"]
                    cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','akrishnan@ajww.com','support@ajwwbo.com']
                    df_mail = df_mail[(df_mail["Destination Country"]!="TR")&(df_mail["Destination Country"]!="IN")]

                df_mail = df_mail[["Shipment ID","Origin","Destination",'ETD','ETA','Date','Mode',"Dept","Job Operator","Controlling Customer","Job Status"]]
                df_mail = df_mail.sort_values("Job Operator", axis = 0, ascending = True,)
                df_mail.to_excel(file_name,index=False)

                msg = EmailMultiAlternatives(subject, table_str, from_email, to, cc=cc)
                msg.attach_alternative(data_table_all, "text/html")
                file_name = "FEA_"+dpt_name + "_Department.xlsx"
                msg.attach_file(file_name)
                msg.send()

            df_filtered = df[df["Job Status"].isin(job_status_filter_names)]
            df_filtered = df_filtered[['Shipment ID',"Controlling Customer Name","Origin","Destination", 'Estimated Delivery Date', 'Actual Delivery Date', 'Mode','Direction' ,'Origin ETD','Destination ETA','Job Opened','Controlling Customer Name','Job Status','Job Sales Rep','Job Operator','Job Dept','Origin','Destination','Mode','Trans']]
            df_filtered['Date'] = df_filtered.apply(date_fill, axis=1)
            operator = pd.read_sql('select * from [dbo].[dimsalesman]',conn)
            operator.rename(columns={'Job Operator':'Job Operator Code'}, inplace=True)
            operator.rename(columns={'FullName':'Job Operator'}, inplace=True)
            df_full = pd.merge(df_filtered, operator, on="Job Operator", how='left')
            print(df_full)
            df_full.columns = ['Shipment ID',"Controlling Customer Name","Origin","Destination", 'Estimated Delivery Date', 'Actual Delivery Date', 'Mode','Direction' , 'ETD','ETA','Job Opened','Controlling Customer','Job Status','Sales Rep','Job Operator','Dept','Origin','Dest.','Cont.','Trans.','Date','Loginname','Job Operator Code','Country','EmailAddress','PBIEmail']
            df_full['FullName'] = df_full['Job Operator']
            df_with_email = df_full
            from datetime import datetime, timedelta, date
            d = datetime.today() - timedelta(days=3)
            df_full = df_full[df_full['Date'] <= d ]
            import datetime
            today = datetime.date.today()
            first = today.replace(day=1)
            lastMonth = first - datetime.timedelta(days=1)
            dat = lastMonth.strftime("%m")
            dat = int(dat)
            print(dat)
            todays_date = date.today()
            year_cuurent = todays_date.year
            df_full['month'] = pd.DatetimeIndex(df_full['Date']).month
            df_full['year'] = pd.DatetimeIndex(df_full['Date']).year
            df_full['month'] = df_full['month'].astype('int32')
            df_full['year']  = df_full['year'].astype('int32')
            df_full.to_excel('Filterd Unbilled Raw Data.xlsx')
            df_val_count1 = df_full[df_full["Dept"]=="FEA"]
            cc_name = ["TURKISH MILITARY"]
            import calendar
            for dpt_name in cc_name:
                data_table_all = """
                    <head>
                    <style type='text/css'>
                    table {
                    border-collapse: collapse;
                    }
                    th {
                    text-align: left !important;
                    padding: 2px;
                    }
                    td {
                    text-align: center !important;
                    padding: 2px;
                    }
                    tr:nth-child(even){background-color: #f2f2f2}
                    th {
                    background-color: #1b30ab;
                    color: #fff;
                    }
                    </style>
                    </head>
                    <p><b>Hi Team</b>  <br/><br/> Please find the below unbilled shipments Kindly expedite the billing process <br/> <br/>Current Date minus three days (ETD  Exports and ETA  Imports ) has been  considered as Unbilled <br/>  <br/>Best Regards <br/>IT Control Tower <br/> <br/>This is an automated unbilled email tigger for any queries on this please reach out kalirajan@ajww.com or sabith@ajww.com or accounts4@ajwwbo.com <br/>"""
                data_table_all  = data_table_all + " <h2>Department of FEA - Country : " + dpt_name +" </h2> <br/>"
                
                df_val_count = df_val_count1[df_val_count1["Controlling Customer Name"]=="TURKISH MILITARY"]
                
                df_val_count = df_val_count[["FullName","month",'year']].value_counts()
                df_value_counts = df_val_count.reset_index()
                df_value_counts.columns = ['FullName','Month',"year","ShipmentCount"]

                df_value_counts["Month Name"] = df_value_counts["Month"]

                df_value_counts = df_value_counts.sort_values(['year','Month'],ascending=[True, True])

                df_value_counts['Month Name'] = df_value_counts['Month Name'].apply(lambda x: str(x)+"."+calendar.month_abbr[x])

                cols = ['Month Name', 'year']

                df_value_counts['Month Name'] = df_value_counts[cols].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

                table = pd.pivot_table(df_value_counts,index=['FullName',],columns=['year',"Month","Month Name"],values=['ShipmentCount'])
                table = table.fillna(0)

                table.index.name = None

                col_name = []
                for col_name1 in table.columns:
                    col_name.append(col_name1[3])
                table.columns =col_name

                table["Total"] = table.sum(axis=1)
                table.loc['Total']= table.sum()
                table = table.replace(0,'')
                table["Sales Person"] = table.index
                table.insert(0,'Operators',table["Sales Person"])
                del table['Sales Person']
                table = table.reset_index(drop=True)

                print(table)

                df_sales_person_email = list(df_with_email[df_with_email['Dept']==dpt_name]['EmailAddress'].unique())
                # data_table_all = data_table_all + str(df_sales_person_email)

                data_table_all = data_table_all + table.to_html(index=False)
                data_table_all = data_table_all + "<br/><br/><br/>"
                data_table_all=data_table_all.replace('\n','')
                table_str = table.to_string()
                table_str = table_str.replace('\n','').strip()
                # data_table_all = data_table_all + data_tbl
                data_table_all = str(data_table_all)
                data_table_all = data_table_all.replace('11.','')
                data_table_all = data_table_all.replace('12.','')
                data_table_all = data_table_all.replace('1.','')
                data_table_all = data_table_all.replace('2.','')
                data_table_all = data_table_all.replace('3.','')
                data_table_all = data_table_all.replace('4.','')
                data_table_all = data_table_all.replace('5.','')
                data_table_all = data_table_all.replace('6.','')
                data_table_all = data_table_all.replace('7.','')
                data_table_all = data_table_all.replace('8.','')
                data_table_all = data_table_all.replace('9.','')
                data_table_all = data_table_all.replace('10.','')
                print(data_table_all)

                file_name = "FEA_"+dpt_name+"_Department.xlsx"
                
                df_mail = df_full[df_full["Dept"]=="FEA"]
                df_mail = df_mail[df_mail["Controlling Customer Name"]==dpt_name]

                df_mail = df_mail[["Shipment ID","Origin","Destination",'ETD','ETA','Date','Mode',"Dept","Job Operator","Controlling Customer","Job Status"]]
                df_mail = df_mail.sort_values("Job Operator", axis = 0, ascending = True,)
                df_mail.to_excel(file_name,index=False)

                from datetime import datetime
                today_date_sub = datetime.today().strftime('%d-%m-%Y')
                subject = "Unbilled Trigger FEA Department - Controlling Customer "+str(dpt_name) + "-" + str(today_date_sub)
                from_email = settings.EMAIL_HOST_USER

                to=["steve@ajww.com"]
                cc = ['kalirajan@ajww.com','sabith@ajww.com','jayachandran@ajww.com','accounts4@ajwwbo.com','support@ajwwbo.com','akrishnan@ajww.com']
                
                msg = EmailMultiAlternatives(subject, table_str, from_email, to, cc=cc)
                msg.attach_alternative(data_table_all, "text/html")
                file_name = "FEA_"+dpt_name + "_Department.xlsx"
                msg.attach_file(file_name)
                msg.send()

        else:
            pass

    return render(request,'dashboard.html')





def handle_uploaded_file(f):
    with open('data_sheet.xlsx', 'wb+') as destination:  
        for chunk in f.chunks():  
            destination.write(chunk)  

# Billed Data

# Billed Data

import pandas as pd
import pyodbc
import datetime
from datetime import date
import calendar
import numpy as np
from datetime import datetime as dt

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)
pd.options.display.float_format = "{:,.0f}".format

def date_fill1(row):
    start = dt.strptime(row["Job Revenue Recognition Date"], '%Y-%m-%d')
    start = start.date()
    end = dt.strptime(row["Date"], '%Y-%m-%d')
    end = end.date()
    days = np.busday_count(end , start )
    return days

def del_status(row):
    unbilled_list = ["WRK","WHL","IHL"]
    if row["Job Status"] in unbilled_list:
        return "Unbilled"
    elif row["#Days (Excl Weekend)"] <= 3:
        return "OnTime"
    else:
        return "Delayed"


# For Billed Parameter Analysis
@login_required
def mail_billed_data(request):
    # Reading Raw Data
    df = pd.read_excel('data_sheet.xlsx',skiprows=14)
    df["SAJF"] = df["Shipment ID"].str.startswith("SAJF", na = False)
    df = df[df["SAJF"]==True]
    df_status_list = ["WRK","WHL","IHL"]
    job_status_filter_names = []
    for li in df["Job Status"].unique().tolist():
        if li in df_status_list:
            pass
        else:
            job_status_filter_names.append(li)

    # Filtered Data based on job status
    df_filtered = df[df["Job Status"].isin(job_status_filter_names)]    
    df_filtered = df[['Shipment ID', 'Actual Delivery Date', 'Estimated Delivery Date', 'Mode','Direction' ,'Origin ETD','Destination ETA','Job Revenue Recognition Date' ,'Job Opened','Controlling Customer Name','Job Status','Job Sales Rep','Job Operator','Job Dept','Origin','Destination','Mode','Trans']]
    df_filtered['Date'] = df_filtered.apply(date_fill, axis=1)
    
    # Getting operator details from database
    operator = pd.read_sql('select * from [dbo].[dimsalesman]',conn)
    operator.rename(columns={'Job Operator':'Job Operator Code'}, inplace=True)
    operator.rename(columns={'FullName':'Job Operator'}, inplace=True)

    # merging Filterd data and operator Data
    df_full = pd.merge(df_filtered, operator, on="Job Operator", how='left')
    df_full.columns = ['Shipment ID', 'Actual Delivery Date', 'Estimated Delivery Date', 'Mode','Direction' , 'ETD','ETA','Job Revenue Recognition Date','Job Opened','Controlling Customer','Job Status','Sales Rep','Job Operator','Dept','Origin','Dest.','Cont.','Trans.','Date','Loginname','Job Operator Code','Country','EmailAddress','PBIEmail']
    df_full['FullName'] = df_full['Job Operator']
    df_full["Job Revenue Recognition Date"] =df_full["Job Revenue Recognition Date"].str.split(' ').str[1]
    df_full['Date'] = df_full['Date'].dt.normalize()
    df_full[["Job Revenue Recognition Date", "Date"]] = df_full[["Job Revenue Recognition Date", "Date"]].apply(pd.to_datetime)
    
    # Filling Job Revenue Recognition Date
    df_full["Job Revenue Recognition Date"] = df_full["Job Revenue Recognition Date"].fillna('1970-01-01 00:00:00')
    df_full["Date"] = df_full["Date"].fillna('1970-01-01 00:00:00')

    # Converting Datatype of Job Revenue Recognition Date column
    df_full["Job Revenue Recognition Date"]=pd.to_datetime(df_full['Job Revenue Recognition Date'])
    df_full["Date"]=pd.to_datetime(df_full['Date'])

    df_full["Job Revenue Recognition Date"]=pd.to_datetime(df_full['Job Revenue Recognition Date']).astype(str)
    df_full["Date"]=pd.to_datetime(df_full['Date']).astype(str)
    df_full['#Days (Excl Weekend)'] = df_full.apply(date_fill1, axis=1)
    df_full["Delayed_Status"] = df_full.apply(del_status,axis=1)
    import datetime
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    dat = lastMonth.strftime("%m")
    dat = int(dat)

    todays_date = datetime.date.today()
    year_cuurent = todays_date.year

    df_full['Date'] = pd.DatetimeIndex(df_full['Date'])
    df_full['month'] = pd.DatetimeIndex(df_full['Date']).month
    df_full['year'] = pd.DatetimeIndex(df_full['Date']).year
    df_full['month'] = df_full  ['month'].astype('int32')
    df_full['year']  = df_full['year'].astype('int32')
    pd.options.display.float_format = "{:,.0f}".format

    # Creating copy of df_full and storing it into df_billed
    df_billed = df_full.copy(deep=True)

    # Converting Names of Departments

    def index_names(row):
        if row["Dept"] == "FDR":
            return "TRANSPORT"

        if row["Dept"] == "FEA":
            return "AIR EXPORT"

        if row["Dept"] == "FES":
            return "OCEAN EXPORT"

        if row["Dept"] == "FIA":
            return "AIR IMPORT"

        if row["Dept"] == "FIS":
            return "OCEAN IMPORT"

        if row["Dept"] == "MSC":
            return "SERVICE JOBS"

        if row["Dept"] == "WFS":
            return "WAREHOUSE"

        if row["Dept"] == "WFW":
            return "WAREHOUSE"

    df_billed["Dept"] = df_billed.apply(index_names,axis=1)

    # Checking shipment Ontime
    def ontime_days(row):
        unbilled_list = ["WRK","WHL","IHL"]
        if (row["#Days (Excl Weekend)"] <= 3) and (row["Job Status"] not in unbilled_list):
            return 1
        else:
            return 0
        
    df_billed["ONTIME"] =df_billed.apply(ontime_days,axis=1)

    # Checking shipment Delayed Days
    def delay_days(row):
        unbilled_list = ["WRK","WHL","IHL"]
        if (row["#Days (Excl Weekend)"] > 3) and (row["Job Status"] not in unbilled_list):
            return 1
        else:
            return 0
    df_billed["DELAYED"] =df_billed.apply(delay_days,axis=1)

    # Checking If Shipment is Unbilled
    def Unbilled_days(row):
        if row["Delayed_Status"]=="Unbilled":
            return 1
        else:
            return 0
    df_billed["UNBILLED"] =df_billed.apply(Unbilled_days,axis=1)

    def Total_days(row):
        return row["ONTIME"] + row["DELAYED"] + row["UNBILLED"]
    df_billed["TOTAL"] =df_billed.apply(Total_days,axis=1)

    df_billed["Month Name"] = df_billed["month"]
    df_billed = df_billed.sort_values(['year','month'],ascending=[True, True])
    df_billed['Month Name'] = df_billed['Month Name'].apply(lambda x: str(x)+"."+calendar.month_abbr[x])
    cols = ['Month Name', 'year']
    df_billed['Month Name'] = df_billed[cols].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

    html_data = """
                    <head>
                    <style type='text/css'>
                    table {
                    border-collapse: collapse;
                    }
                    th {
                    text-align: left !important;
                    padding: 2px;
                    }
                    td {
                    text-align: center !important;
                    padding: 2px;
                    }
                    tr:nth-child(even){background-color: #f2f2f2}
                    th {
                    background-color: #5172e0;
                    color: white;
                    }
                    #Ontime_tbl th{
                    background-color: #3be36b;
                    color: white;
                    }
                    </style>
                    </head>
                    <p><b>
                    Dear All, <br/><br/>
                        Please find the below Table Billing Parameter analysis for each Segment wise. <br/>
                        3 days norms has been considered excluding Sat and Sun. Imports Team please ensure EST.DELIVERY / Actual Delivery date are captured. <br/>
                        This is an automated billed email trigger for any queries on this please reach out support@ajwwbo.com or sabith@ajww.com or accounts4@ajwwbo.com
    """

    # Checking total Shipment
    def total_ship(row):
        df_1 = df_billed[(df_billed["year"] == int(row["year"]))&(df_billed["month"]==int(row["month"]))&(df_billed["Dept"]==row["Dept"])]
        return len(df_1)

    # Checking ontime Shipment
    def ontime_ship(row):
        df_1 = df_billed[(df_billed["year"] == int(row["year"]))&(df_billed["month"]==int(row["month"]))&(df_billed["Dept"]==row["Dept"])&(df_billed["Delayed_Status"]=="OnTime")]
        return len(df_1)

    # Checking Ontime Percentage
    def ontime_percent_ship(row):
        if row["Ontime"] == 0:
            return 0
        else:
            val = (row["Ontime"] / row["Total"])*100
            val = str(val)
            val = val.split('.')[0]
            val = val + str(" %")
            return val

    month_list = []
    current_month = datetime.datetime.today()
    current_month_minus_1 = current_month - dateutil.relativedelta.relativedelta(months=1)
    current_month_minus_2 = current_month - dateutil.relativedelta.relativedelta(months=2)

    year_list = df_billed["year"].unique().tolist()
    year_list = sorted(year_list)

    today_date_for_month = datetime.date.today()
    dat1 = today_date_for_month.strftime("%m")
    dat1 = int(dat1)
    month_list = [i for i in range(1,dat1+1)]

    # Creating Department List
    dept_list = ['TRANSPORT','OCEAN IMPORT', 'OCEAN EXPORT', 'AIR EXPORT', 'AIR IMPORT']

    data_for_new_df = {"year":[],"month":[],"Dept":[]}
    df_new_1 = pd.DataFrame(data_for_new_df)
    year_list = [current_month.year]
    for a in year_list:
        for b in month_list:
            for c in dept_list:
                data_row_list = [str(a),b,c]
                df_new_1.loc[len(df_new_1)] = data_row_list
    df_new_1.columns
    df_new_1["Total"] = df_new_1.apply(total_ship,axis=1)
    df_new_1["Ontime"] = df_new_1.apply(ontime_ship,axis=1)
    df_new_1["Ontime Percent"] = df_new_1.apply(ontime_percent_ship,axis=1)

    # Creating pivot_table for ontime Percentage
    table_piv_ontime = pd.pivot_table(df_new_1,index=['Dept'],columns=["year","month"],values=['Ontime Percent'],aggfunc='sum')

    col_name_ontime = []
    for col_name_ontime_i in table_piv_ontime.columns:
        col_name_ontime.append(col_name_ontime_i[2])
    table_piv_ontime.columns =col_name_ontime
    table_piv_ontime = table_piv_ontime.fillna(' ')
    table_piv_ontime.index.name = None
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    month_names_use = [month_names[i] for i in range(0,dat1) ]
    table_piv_ontime.columns = month_names_use

    html_data = html_data + "<br/><h2> Monthwise ONTIME % - "+str(current_month.year)+"  </h2>"
    html_data = html_data + str(table_piv_ontime.to_html())
    html_data = html_data + "<br/>"

    month_list = []
    # Creating Df for Last Three months
    for i in range(0,3):
        if i == 2:
            month_list.append(int(current_month_minus_2.month))
            df_full_month_data = df_billed[df_billed["year"]==current_month_minus_2.year]
            df_full_month_data = df_full_month_data[df_full_month_data["month"]==int(current_month_minus_2.month)]
            df_full_month_data.to_excel("df_full_month_data.xlsx")
            table_piv = pd.pivot_table(df_full_month_data,index=['Dept'],columns=["month"],values=['ONTIME','DELAYED','UNBILLED','TOTAL'],aggfunc='sum')
            table_piv.index.name = None
            col_name = []
            for col_name1 in table_piv.columns:
                col_name.append(col_name1[0])
            table_piv.columns =col_name
            table_piv = table_piv.fillna(' ')
            def Ontime_percent(row):
                val = (row["ONTIME"] / row["TOTAL"])*100
                val = str(val)
                val = val.split('.')[0]
                val = val + str("%")
                return val
            table_piv.loc['GRAND TOTAL',:]= table_piv.sum(axis=0)
            table_piv["ONTIME %"] = table_piv.apply(Ontime_percent,axis=1)
            table_piv = table_piv[["ONTIME","DELAYED",'UNBILLED','TOTAL','ONTIME %']]
            monthinteger = int(current_month_minus_2.month)
            import datetime
            month = datetime.date(1900, monthinteger, 1).strftime('%B')
            head = "<h2>"+str(month) + " " +str(current_month_minus_2.year)+"</h2>"
            html_data = html_data + head
            html_data = html_data + str(table_piv.to_html())
            print(head)
            print(table_piv)

        elif i == 1:
            month_list.append(int(current_month_minus_1.month))
            df_full_month_data = df_billed[df_billed["year"]==current_month_minus_1.year]
            df_full_month_data = df_full_month_data[df_full_month_data["month"]==int(current_month_minus_1.month)]
            table_piv = pd.pivot_table(df_full_month_data,index=['Dept'],columns=["Month Name"],values=['ONTIME','DELAYED','UNBILLED','TOTAL'],aggfunc='sum')
            table_piv.index.name = None
            col_name = []
            for col_name1 in table_piv.columns:
                col_name.append(col_name1[0])
            table_piv.columns =col_name
            table_piv = table_piv.fillna(' ')
            def Ontime_percent(row):
                val = (row["ONTIME"] / row["TOTAL"])*100
                val = str(val)
                val = val.split('.')[0]
                val = val + str("%")
                return val
            table_piv.loc['GRAND TOTAL',:]= table_piv.sum(axis=0)
            table_piv["ONTIME %"] = table_piv.apply(Ontime_percent,axis=1)
            table_piv = table_piv[["ONTIME","DELAYED",'UNBILLED','TOTAL','ONTIME %']]
            monthinteger = int(current_month_minus_1.month)
            import datetime
            month = datetime.date(1900, monthinteger, 1).strftime('%B')
            head = "<h2>"+str(month) + " " +str(current_month_minus_1.year)+"</h2>"
            print(head)
            html_data = html_data + head
            html_data = html_data + str(table_piv.to_html())
            print(table_piv)

        elif i == 0:
            month_list.append(int(current_month.month))
            df_full_month_data = df_billed[df_billed["year"]==current_month.year]
            df_full_month_data = df_full_month_data[df_full_month_data["month"]==int(current_month.month)]
            table_piv = pd.pivot_table(df_full_month_data,index=['Dept'],columns=["Month Name"],values=['ONTIME','DELAYED','UNBILLED','TOTAL'],aggfunc='sum')
            table_piv.index.name = None
            col_name = []
            for col_name1 in table_piv.columns:
                col_name.append(col_name1[0])
            table_piv.columns =col_name
            table_piv = table_piv.fillna(' ')
            def Ontime_percent(row):
                val = (row["ONTIME"] / row["TOTAL"])*100
                val = str(val)
                val = val.split('.')[0]
                val = val + str("%")
                return val
            table_piv.loc['GRAND TOTAL',:]= table_piv.sum(axis=0)
            table_piv["ONTIME %"] = table_piv.apply(Ontime_percent,axis=1)
            table_piv = table_piv[["ONTIME","DELAYED",'UNBILLED','TOTAL','ONTIME %']]
            monthinteger = int(current_month.month)
            import datetime
            month = datetime.date(1900, monthinteger, 1).strftime('%B')
            head = "<h2>"+str(month) + " " +str(current_month.year)+"</h2>"
            print(head)
            html_data = html_data + head
            html_data = html_data + str(table_piv.to_html())
            print(table_piv)

    html_data = html_data + " <br/>  <br/>  <br/> Best Regards <br/> IT Control Tower"
    subject = "Billing Parameter Analysis -  " + str(todays_date)
    from_email = settings.EMAIL_HOST_USER
    
    df_full_data = df_full[df_full["month"].isin(month_list)] 
    del df_full_data["Controlling Customer"]
    del df_full_data["Sales Rep"]

    del df_full_data["Origin"]

    del df_full_data["Dest."]
    del df_full_data["Country"]

    def bucket_days(row):
        if row["#Days (Excl Weekend)"] <= 0:
            return "<= 0 Days"
        elif row["#Days (Excl Weekend)"] > 0 and row["#Days (Excl Weekend)"] <= 3 :
            return "0 - 3 Days"
        elif row["#Days (Excl Weekend)"] > 3 and row["#Days (Excl Weekend)"] <= 5 :
            return "3 - 5 Days"
        elif row["#Days (Excl Weekend)"] > 5 and row["#Days (Excl Weekend)"] <= 7 :
            return "5 - 7 Days"
        elif row["#Days (Excl Weekend)"] > 7 and row["#Days (Excl Weekend)"] <= 10 :
            return "7 - 10 Days"
        elif row["#Days (Excl Weekend)"] > 10 and row["#Days (Excl Weekend)"] <= 30 :
            return "10+ Days"
        elif row["#Days (Excl Weekend)"] > 30:
            return "30+ Days"
        else:
            return "Not Availlable"


    df_full_data["Bucket Days"] =  df_full_data.apply(bucket_days,axis=1)
    df_full_data.to_excel("Raw_data_billed.xlsx",index=False)

    to = ['kalirajan@ajww.com','support@ajwwbo.com']
    #to = ['support@ajwwbo.com']
    cc =  ['itc@ajww.com']
    # to =["peter@ajww.com", "philips@ajww.com", "sathish@ajww.com", "danny@ajww.com", "melvin@ajww.com", "victorb@ajww.com", "vicky@ajww.com", "cecil@ajww.com", "steve@ajww.com", "shipaj@ajww.com", "sarath@ajww.com", "sinem@ajww.com", "canberk@ajww.com", "emre@ajww.com", "vivien@ajww.com"]
    # cc = ["ndale@ajww.com", "jayachandran@ajww.com", "sabith@ajww.com", "kalirajan@ajww.com", "accounts4@ajwwbo.com"]
    # to = ["danny@ajww.com","sathish@ajww.com","rick@ajww.com","cecil@ajww.com","vicky@ajww.com","sinem@ajww.com","vivien@ajww.com","onur@ajww.com","shipaj@ajww.com","metin@ajww.com","filiz@ajww.com","arda@ajww.com","sarath@ajww.com","steve@ajww.com","emre@ajww.com"]
    # cc = ["jayachandran@ajww.com", "sabith@ajww.com", "kalirajan@ajww.com", "accounts4@ajwwbo.com",'support@ajwwbo.com']
    msg = EmailMultiAlternatives(subject, html_data, from_email, to,cc=cc)
    msg.attach_alternative(html_data, "text/html")
    msg.attach_file("Raw_data_billed.xlsx")
    msg.send()
    return render(request,'dashboard.html')




    
def add_new_operator(request):
    return render(request,'add_new_operator.html')


# For add New user
def add_new_operator_ajax(request):
    if request.method == "POST":
        opname = request.POST["opname"]
        loginname = request.POST["loginname"]
        Code = request.POST["Code"]
        Country = request.POST["Country"]
        email = request.POST["email"]
        pbiemail = request.POST["pbiemail"]
        sql = "insert into [dbo].[dimsalesman](FullName,Loginname,Code,Country,EmailAddress,PBIEmail) VALUES(?, ?, ?, ?, ?, ?)"
        params = (opname,loginname,Code,Country,email,pbiemail)
        cursor = conn.cursor()
        cursor.execute(sql,params)
        print(opname , " - Record Inserted")
        conn.commit()
        # insert into [dbo].[dimsalesman](FullName,Loginname,Code,Country,EmailAddress,PBIEmail) values('Kamuran Dogan','dogan@ajww.com');
    return render(request,'add_new_operator.html')


# for getting non email operator list
def get_non_email_op_list(request):
    if request.method == "POST":
        df = pd.read_excel('data_sheet.xlsx',skiprows =14)
        del df['Unnamed: 0']
        del df['Unnamed: 1']
        job_status_filter_names = ["WRK","WHL","IHL"]
        df_filtered = df[df["Job Status"].isin(job_status_filter_names)]
        df_filtered = df_filtered[['Shipment ID', 'Mode','Direction' ,'Origin ETD','Destination ETA','Job Opened','Controlling Customer Name','Job Status','Job Sales Rep','Job Operator','Job Dept','Origin','Destination','Mode','Trans']]
        operator = pd.read_sql('select * from [dbo].[dimsalesman]',conn)
        operator.rename(columns={'Job Operator':'Job Operator Code'}, inplace=True)
        operator.rename(columns={'FullName':'Job Operator'}, inplace=True)
        df_full = pd.merge(df_filtered, operator, on="Job Operator",how="left")
        df_full_nonEmailUser = df_full["EmailAddress"].fillna("")
        df_full_nonEmailUser = df_full[df_full["EmailAddress"]==""]
        df_full_nonEmailUser = df_full_nonEmailUser[["Job Operator","Job Sales Rep"]]
        df_full_nonEmailUser = df_full_nonEmailUser.reset_index()
        df_full_nonEmailUser = df_full_nonEmailUser.rename(columns={"Job Operator": "JobOperator", "Job Sales Rep": "JobSalesRep"})
        if len(df_full_nonEmailUser)>0:
            df_full_nonEmailUser = df_full_nonEmailUser.to_json(orient='records')

        else:
            df_full_nonEmailUser = [{
                "JobOperator":"Not Founded",
                "JobSalesRep":"Not Founded"
            }]
            df_full_nonEmailUser = json.dumps(df_full_nonEmailUser)
        return HttpResponse(df_full_nonEmailUser)
