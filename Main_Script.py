#!/usr/bin/env python
# coding: utf-8
import Processing
import Cleaning
import pandas as pd
import numpy as np
import os
import Smartsheets
from Smartsheets import SmartsheetManager
import importlib
importlib.reload(Smartsheets)

api_token = os.environ['SMARTSHEET_ACCESS_TOKEN']
sheet_manager = SmartsheetManager(api_token)

Bus_Requirement = sheet_manager.fetch_data('3452528523956100')
Pullout_Delivery_Operations = sheet_manager.fetch_data('5882897307225988')
Bus_Details = sheet_manager.fetch_data('1431443615993732')
Operations_Report = sheet_manager.fetch_data('3294188112138116')
Missing_Blocks = sheet_manager.fetch_data('3905795481620356')
Route_Times = sheet_manager.fetch_data('3318406627413892')

Pullout_Delivery_Tracking = Processing.get_latest_entries_pullout_delivery(Pullout_Delivery_Operations)

Latest_Entry_Operations = Processing.latest_entry(Operations_Report, 'Service Date ')
Latest_Entry_Bus_Details = Processing.latest_entry(Bus_Details, 'AM Pull-Out Entry Date')
Latest_Entry_Operations['Report Time'] = 'Operations'
Latest_Entry_Bus_Details['Report Time'] = 'Bus Details'
Latest_Entries = pd.concat([
  Pullout_Delivery_Tracking,
  Latest_Entry_Bus_Details,
  Latest_Entry_Operations
])
Latest_Entries['Service Date'] = pd.to_datetime(Latest_Entries['Service Date']).dt.date
Latest_Entries['Flag Late'] = Latest_Entries.apply(Processing.flag_late, axis=1)

Personnel = Pullout_Delivery_Operations[[ 
 'Created',
 'Service Date',
 'Report Time',   
 'Dispatchers - 17th St',
 'Dispatchers - Hains Point',
 'Dispatchers - South Capitol',
 'OCC']]
columns_to_replace = [
    'Dispatchers - 17th St',
    'Dispatchers - Hains Point',
    'Dispatchers - South Capitol',
    'OCC'
]
Personnel[columns_to_replace] = Personnel[columns_to_replace].applymap(lambda x: str(x).replace('"', ''))
Personnel['Service Date'] = pd.to_datetime(Personnel['Service Date'])
Personnel = Personnel[Personnel['Report Time'].notna()]
for col in ['Dispatchers - 17th St',
            'Dispatchers - Hains Point',
            'Dispatchers - South Capitol',
            'OCC']:
    Personnel[col] = Personnel[col].apply(Cleaning.replace_first_comma)
Personnel = Processing.reshape_personnel(Personnel)

Pullout_Delivery_Operations = Pullout_Delivery_Operations.drop(['Created',
                                                                'Created By',
                                                                "Respondent's Name",
                                                                'Dispatchers - 17th St',
                                                                'Dispatchers - Hains Point',
                                                                'Dispatchers - South Capitol',
                                                                'OCC',
                                                                'Anything else you want to add? ',
                                                                'Once you submit the form, '], 
                                                               axis = 1)
Pullout_Delivery_Operations = Cleaning.rename_columns(Pullout_Delivery_Operations)
Pullout_Delivery_Operations.loc[Pullout_Delivery_Operations['Peak Number of Vehicles CH-US'].notna(), 
                                'Report Time'] = 'Operations Report'
Route_Level_Data_Reshaped = Processing.reshape_service_pull(Pullout_Delivery_Operations)
Route_Level_Data_Reshaped = pd.merge(Route_Level_Data_Reshaped, 
                                     Bus_Requirement,
                                     how = 'left', 
                                     on = ['Route', 
                                           'Report Time', 
                                           'Service Week'])
Overall_Service_Data = Pullout_Delivery_Operations[[
 'Last Modified Time',
 'Service Date',
 'Report Time',
 'Spares',
 'Down',
 'Training',
 'Not in Service',
 'Operators Scheduled',
 'Total Operators Call-Outs',
 'Total Operators Scheduled Off',
 'Proterra Bus Low Charge',
 'Proterra 80% to Full Charge',
 'Proterra Chargers Down',
 'Buses Currently in Service']]
Overall_Service_Data.loc[Overall_Service_Data['Report Time'] == 'Night Service Delivery (9 PM onwards)', 
                         'Buses Currently in Service'] = None 
Overall_Service_Data.loc[Overall_Service_Data['Total Operators Call-Outs'].notna(), 
                         'Report Time'] = 'Operations Report'
Overall_Service_Data['Service Date'] = pd.to_datetime(Overall_Service_Data['Service Date'])
Route_Level_Data_Reshaped = pd.merge(Route_Level_Data_Reshaped,
                                     Overall_Service_Data,
                                     how = 'left',
                                     on = ['Last Modified Time', 
                                          'Service Date',
                                          'Report Time'])
Route_Level_Data_Reshaped = Route_Level_Data_Reshaped.drop('Buses Currently in Service', axis =1)
Route_Level_Data_Reshaped['Road Supervisors'] = Route_Level_Data_Reshaped['Road Supervisors'].str.replace('"','')

route_supervisors = Route_Level_Data_Reshaped[[
    'Last Modified Time',
    'Service Date',
    'Report Time',
    'Route',
    'Road Supervisors'
]][Route_Level_Data_Reshaped['Report Time'] != 'Operations Report']
route_supervisors['Route_Supervisor_Count'] = route_supervisors['Road Supervisors'].apply(Cleaning.count_names)
route_supervisors['Road Supervisors'] = route_supervisors['Road Supervisors'].apply(Cleaning.replace_first_comma)
route_supervisors['Service Date'] = pd.to_datetime(route_supervisors['Service Date'])

Route_Level_Data_Reshaped['Detour Details'] = Route_Level_Data_Reshaped['Detour Details'].apply(Cleaning.format_details)
Route_Level_Data_Reshaped = Route_Level_Data_Reshaped.drop('Road Supervisors', axis = 1)
Route_Level_Data_Reshaped['Last Modified Time'] = pd.to_datetime(Route_Level_Data_Reshaped['Last Modified Time'])
Route_Level_Data_Reshaped['Last Modified Time'] = Route_Level_Data_Reshaped['Last Modified Time'].apply(Cleaning.convert_timestamp)
col_to_numeric = [
 'Available Operators',
 'Available Vehicles',
 'Peak Number of Vehicles',
 'Buses Required',
 'Spares',
 'Down',
 'Training',
 'Not in Service',
 'Operators Scheduled',
 'Total Operators Call-Outs',
 'Total Operators Scheduled Off',
 'Proterra Bus Low Charge',
 'Proterra 80% to Full Charge',
 'Proterra Chargers Down'
]
Route_Level_Data_Reshaped[col_to_numeric] = Route_Level_Data_Reshaped[col_to_numeric].apply(pd.to_numeric)
Route_Level_Data_Reshaped = Route_Level_Data_Reshaped.groupby(['Service Date',
                                                               'Report Time', 
                                                               'Route']).first().reset_index()
Route_Level_Data_Reshaped['Total Proterra Buses'] = 15
Route_Level_Data_Reshaped['Total Bus Fleet'] = 73
Route_Level_Data_Reshaped['Total Chargers'] = 15
condition_to_remove = (Route_Level_Data_Reshaped['Service Date'] <= '2023-08-07') & (Route_Level_Data_Reshaped['Report Time'] == 'Operations Report')
Route_Level_Data_Reshaped = Route_Level_Data_Reshaped[~condition_to_remove]

Service_Pull = Route_Level_Data_Reshaped[[
 'Service Date',
 'Report Time',
 'Service Week',
 'Route',
 'Available Vehicles',
 'Peak Number of Vehicles',
 'Buses Required',   
 'Detour',
 'Detour Details'
]]
mask = Service_Pull['Report Time'] == 'Operations Report'
Service_Pull.loc[mask,'Available Vehicles'] = Service_Pull.loc[mask,'Peak Number of Vehicles']
Service_Pull = Processing.update_operations_report(Service_Pull, 
                                       'Buses Required',
                                       'PM Service Delivery (3 PM-5 PM)')
Service_Pull = Processing.update_operations_report(Service_Pull, 
                                       'Detour',
                                       'PM Service Delivery (3 PM-5 PM)')
Service_Pull = Processing.update_operations_report(Service_Pull, 
                                       'Detour Details',
                                       'PM Service Delivery (3 PM-5 PM)')
Service_Pull['Total Available Vehicles'] = Service_Pull.groupby(['Service Date', 'Report Time'])['Available Vehicles'].transform('sum')
Service_Pull['Total Required Vehicles'] = Service_Pull.groupby(['Service Date', 'Report Time'])['Buses Required'].transform('sum')
Service_Pull['Bus Fleet Compliance Rate'] =  Processing.proportion(Service_Pull,'Total Available Vehicles','Total Required Vehicles')      
Service_Pull = Processing.reformatting_columns(Service_Pull)

Operators_Data = Route_Level_Data_Reshaped[[
'Service Date',
'Report Time',
'Route', 
'Service Week',
'Available Operators',
'Operators Scheduled',
'Total Operators Call-Outs',
'Total Operators Scheduled Off',
]]
mask = (Operators_Data['Report Time'] == 'Operations Report') | (Operators_Data['Report Time'] == 'Pull-out AM (6 AM-7 AM)')
Operators_Data = Operators_Data[mask]
Operators_Data =  Processing.update_operations_report(Operators_Data, 
                                       'Available Operators',
                                       'Pull-out AM (6 AM-7 AM)')
Operators_Data =  Processing.update_operations_report(Operators_Data, 
                                       'Operators Scheduled',
                                       'Pull-out AM (6 AM-7 AM)')
Operators_Data = Operators_Data[Operators_Data['Report Time'] == 'Operations Report']
Operators_Data = Operators_Data.drop('Report Time',axis =1)
Operators_Data['Total Operators'] = Operators_Data.groupby('Service Date')['Available Operators'].transform('sum')
Operators_Data['Operators Compliance Rate'] = Processing.proportion(Operators_Data,
                                                         'Total Operators',
                                                         'Operators Scheduled')  
Operators_Data['Operators Call-Outs %'] = -Processing.proportion(Operators_Data,
                                                      'Total Operators Call-Outs',
                                                      'Operators Scheduled')  

Total_Bus_Fleet = Route_Level_Data_Reshaped[[
 'Service Date',
 'Report Time',
 'Route',
 'Service Week',
 'Down',
 'Spares',
 'Training',
 'Not in Service',
 'Total Bus Fleet'  
]]
Total_Bus_Fleet['Spares %'] = Processing.proportion(Total_Bus_Fleet, 'Spares','Total Bus Fleet')
Total_Bus_Fleet['Down %'] = Processing.proportion(Total_Bus_Fleet,'Down','Total Bus Fleet')
Total_Bus_Fleet['Training %'] = Processing.proportion(Total_Bus_Fleet,'Training','Total Bus Fleet')    
Total_Bus_Fleet['Not in Service %'] = Processing.proportion(Total_Bus_Fleet,'Not in Service','Total Bus Fleet')                                                                                           
Total_Bus_Fleet = Processing.reformatting_columns(Total_Bus_Fleet)

Low_Charge_Buses = Route_Level_Data_Reshaped[[
 'Service Date',
 'Report Time',
 'Service Week',
 'Proterra Bus Low Charge',
 'Total Proterra Buses'
]][Route_Level_Data_Reshaped['Report Time'] != 'Operations Report']
Low_Charge_Buses['Low Charge Buses %'] = Processing.proportion(Low_Charge_Buses,
                                                    'Proterra Bus Low Charge',
                                                    'Total Proterra Buses') 

Chargers_Full_Buses = Route_Level_Data_Reshaped[[
 'Service Date',
 'Report Time',
 'Service Week',
 'Proterra 80% to Full Charge',
 'Proterra Chargers Down',
 'Total Proterra Buses'
]][Route_Level_Data_Reshaped['Report Time'] == 'Pull-out AM (6 AM-7 AM)']
Chargers_Full_Buses['Chargers Down %'] = Processing.proportion(Chargers_Full_Buses,
                                         'Proterra Chargers Down',
                                         'Total Proterra Buses') 
Chargers_Full_Buses['Full Charge Buses %'] = Processing.proportion(Chargers_Full_Buses,
                                         'Proterra 80% to Full Charge',
                                         'Total Proterra Buses') 


Bus_Details["Today's Date"] = Bus_Details["Today's Date"].apply(Cleaning.convert_timestamp)
Bus_Details = Bus_Details.drop(["Dispatcher's Name",
                                'Name'], 
                              axis = 1)
Bus_Details['AM Pull-Out Entry Date'] = pd.to_datetime(Bus_Details['AM Pull-Out Entry Date'])
Bus_Details = Bus_Details.sort_values(['AM Pull-Out Entry Date', "Today's Date"])
Bus_Details = Cleaning.drop_duplicates_within_range(Bus_Details, min_number=1130, max_number=3101)
Bus_Details = Bus_Details[Bus_Details['AM Pull-out Route/Down/NIS'].notna()]
Bus_Details['AM Pull-out Route/Down/NIS'] = Bus_Details['AM Pull-out Route/Down/NIS'].apply(Cleaning.replace_with_option)
mask = ((Bus_Details['AM Pull-out Route/Down/NIS'] == 'Down') | (Bus_Details['AM Pull-out Route/Down/NIS'] == 'Not in Service'))
Bus_Details.loc[mask & (Bus_Details['Reason for Down Service'].isna()), 
                'Reason for Down Service'] = 'Not Specified'
Bus_Details = Processing.delimiting_multiple_down_reasons(Bus_Details, 'AM Pullout')
Bus_Details['Specify Other Reason Here'][Bus_Details['Specify Other Reason Here'].notna()] = Bus_Details['Specify Other Reason Here'][Bus_Details['Specify Other Reason Here'].notna()].apply(Cleaning.capitalize_each_word)
Bus_Details = Processing.replace_other_with_reason(Bus_Details)
Bus_Details = Bus_Details.drop('Specify Other Reason Here', axis = 1)
Bus_Details = Processing.reformatting_columns(Bus_Details)

Operations_Report = Operations_Report.rename(columns = {'Service Date ' : 'Service Date'})
Operations_Report = Operations_Report[Operations_Report['Service Date'].notna()]
Operations_Report['Route/Down/NIS'] = Operations_Report['Route/Down/NIS'].str.replace('NIS', 
                                                                                      'Not in Service')
exclude_values = ['CH-US', 'GT-US', 'WP-AM', 'RS-DP', 'EM-LP', 'NMS']
for index, row in Operations_Report.iterrows():
    if row['Route/Down/NIS'] not in exclude_values:
        Operations_Report.at[index, 'Route/Down/NIS'] = Cleaning.capitalize_each_word(row['Route/Down/NIS'])
Operations_Report['Service Date'] = pd.to_datetime(Operations_Report['Service Date'])
Operations_Report["Today's Date"] = pd.to_datetime(Operations_Report["Today's Date"])
Operations_Report_Bus_Details = Operations_Report[Operations_Report['Route/Down/NIS'].notna()]
Operations_Report_Bus_Details = Operations_Report_Bus_Details.drop("Manager's Name", axis = 1)
Operations_Report_Bus_Details = Operations_Report_Bus_Details.loc[:, :'Specify Other Status Here']
Operations_Report_Bus_Details = Operations_Report_Bus_Details.drop_duplicates()
Operations_Report_Bus_Details = Operations_Report_Bus_Details.groupby(['Bus Number','Service Date']).first().reset_index()
Operations_Report_Bus_Details = Processing.delimiting_multiple_down_reasons(Operations_Report_Bus_Details, 'Operations')
Operations_Report_Bus_Details = Processing.replace_other_with_reason(Operations_Report_Bus_Details)
for col in ['Current Status for Down Buses', 'Specify Other Status Here']:
       Operations_Report_Bus_Details[col] = Operations_Report_Bus_Details.apply(lambda row: row['Specify Other Status Here'] if row[col] == 'Other' else row[col], axis=1)
Operations_Report_Bus_Details = Operations_Report_Bus_Details.drop('Specify Other Status Here', axis =1)
Operations_Report_Bus_Details['Service Date'] = pd.to_datetime(Operations_Report_Bus_Details['Service Date'])
Operations_Report_Bus_Details['Date When Service Went Down'] = pd.to_datetime(Operations_Report_Bus_Details['Date When Service Went Down'])
Operations_Report_Bus_Details['Expected Return Date'] = pd.to_datetime(Operations_Report_Bus_Details['Expected Return Date'])
Operations_Report_Bus_Details['Down Duration'] = Operations_Report_Bus_Details['Service Date'] - Operations_Report_Bus_Details['Date When Service Went Down']


Bus_Details = Bus_Details.rename(columns={'AM Pull-Out Entry Date':'Service Date'})
Bus_Details_Combined = pd.merge(Operations_Report_Bus_Details, 
                                Bus_Details, on=['Bus Number', 'Service Date'], how='inner')
mask = Bus_Details_Combined['Route/Down/NIS'] == Bus_Details_Combined['AM Pull-out Route/Down/NIS']
Bus_Details_Combined['Bus Status Change'] = 'No' 
Bus_Details_Combined.loc[mask,'Bus Status Change']='Yes'
Bus_Details_Combined['Down Duration'] = Bus_Details_Combined['Down Duration'].dt.days
Bus_Details_Combined = Bus_Details_Combined.drop(["Today's Date_x",
                                                  "Today's Date_y"], axis = 1)
Bus_Details_Combined = Processing.reformatting_columns(Bus_Details_Combined)

Down_NIS_Buses = Bus_Details_Combined[(Bus_Details_Combined['Route/Down/NIS'] == 'Down') |
                                      (Bus_Details_Combined['Route/Down/NIS'] == 'Not In Service')]
Down_NIS_Buses['Number of Days Down'] = Down_NIS_Buses['Down Duration'].astype(str).str.extract('(\d+)').astype(int)


Missed_Revenue = Operations_Report.drop([
  "Manager's Name",
 'Route/Down/NIS',
 'Date When Service Went Down',
 'Bus Went Down on Service Date',
 'Expected Return Date',
 'Reason for Down Service',
 'Specify Other Reason Here',
 'Current Status for Down Buses',
 'Specify Other Status Here'   
], axis = 1)
Missed_Revenue['End Time for Missed Revenue'] = Missed_Revenue['End Time for Missed Revenue'].replace('0:00','24:00')
for index, row in Missed_Revenue.iterrows():
    if row['Reason for Missed Revenue'] is None and row['Other Reason Here'] is not None:
        Missed_Revenue.at[index, 'Reason for Missed Revenue'] = 'Other'
Missed_Revenue = Missed_Revenue[(Missed_Revenue['Missed Revenue Route'].notna()) &
                                (Missed_Revenue['Start Time for Missed Revenue'].notna())]
Missed_Revenue['Other Reason Here'] = Missed_Revenue['Other Reason Here'].apply(Cleaning.capitalize_each_word)
Missed_Revenue = Processing.process_time_columns(Missed_Revenue, 
                                      'Start Time for Missed Revenue', 
                                      'End Time for Missed Revenue',
                                     'Missed Revenue')
Missed_Revenue = Missed_Revenue.rename(columns = {'Service Date ':'Service Date'})
Missed_Revenue['Missed Revenue Duration'] = Missed_Revenue['Missed Revenue Duration'].astype(str)
Missed_Revenue['Missed Revenue Duration'] = Missed_Revenue['Missed Revenue Duration'].str.split(':').apply(lambda x: ':'.join(x[:2]))
Missed_Revenue = pd.merge(Missed_Revenue,
                          Route_Times, 
                          how = 'left', 
                          left_on = 'Missed Revenue Route',
                          right_on = 'Route')
Missed_Revenue['Missed Revenue Duration Seconds'] = Missed_Revenue['Missed Revenue Duration'].apply(Cleaning.convert_time_to_seconds)
Missed_Revenue['Roundtrip Seconds'] = Missed_Revenue['Roundtrip'].apply(Cleaning.convert_time_to_seconds)
Missed_Revenue['Missed Trip Fraction'] = Missed_Revenue['Missed Revenue Duration Seconds'] / Missed_Revenue['Roundtrip Seconds']
Missed_Revenue = Missed_Revenue.drop([ 
 'Route',
 'Roundtrip',
 'Missed Revenue Duration Seconds',
 'Roundtrip Seconds'], axis = 1)
Missed_Revenue = Processing.reformatting_columns(Missed_Revenue)

Missing_Blocks = Missing_Blocks[Missing_Blocks['Route'].notna()]
Missing_Blocks['Created'] = pd.to_datetime(Missing_Blocks['Created']).dt.tz_localize(None)
Missing_Blocks.loc[Missing_Blocks['Service Date'].isna(), 'Service Date'] = Missing_Blocks['Created'] - pd.Timedelta(days=1)
Missing_Blocks['Service Date'] = pd.to_datetime(Missing_Blocks['Service Date'])
Missing_Blocks['Service Date'] = Missing_Blocks['Service Date'].dt.date
Missing_Blocks = Missing_Blocks[(Missing_Blocks['Start Time for Missing Block'].notna()) &
                               (Missing_Blocks['End Time for Missing Block'].notna())]
Missing_Blocks = Processing.process_time_columns(Missing_Blocks, 
                                      'Start Time for Missing Block', 
                                      'End Time for Missing Block',
                                     'Missing Block')
Missing_Blocks['Specify Other Reason Here'][Missing_Blocks['Specify Other Reason Here'].notna()] = Missing_Blocks['Specify Other Reason Here'][Missing_Blocks['Specify Other Reason Here'].notna()].apply(Cleaning.capitalize_each_word)
Missing_Blocks['Reason for Missing Block'] = Missing_Blocks.apply(lambda row: row['Specify Other Reason Here'] if row['Reason for Missing Block'] == 'Other' else row['Reason for Missing Block'],axis=1)
Missing_Blocks = Missing_Blocks.drop(['Specify Other Reason Here',
                                      'primary (no need)'], axis=1)
Missing_Blocks = Processing.reformatting_columns(Missing_Blocks)

dataframes = {
      'Missing_Blocks_Processed New': Missing_Blocks,
      'Missed_Revenue_Processed New': Missed_Revenue,
      'Down_NIS_Buses_Processed New': Down_NIS_Buses,
      'Bus_Details_Combined_Processed New': Bus_Details_Combined,
      'Bus_Details_Processed New': Bus_Details,
      'Chargers_Full_Buses_Processed New': Chargers_Full_Buses,
      'Low_Charge_Buses_Processed New': Low_Charge_Buses,
      'Total_Bus_Fleet_Processed New': Total_Bus_Fleet,
      'Route_Supervisors_Processed New': route_supervisors,
      'Latest_Entries_Processed New': Latest_Entries,
      'Personnel_Processed New': Personnel,
      'Service_Pull_Processed New': Service_Pull,
      'Operators_Data_Processed New': Operators_Data
}

Processing.save_dataframes_to_csv(dataframes)

filenames = {
  'Missing_Blocks_Processed New': 'Missing_Blocks_Processed New.csv',
  'Missed_Revenue_Processed New': 'Missed_Revenue_Processed New.csv',
  'Down_NIS_Buses_Processed New': 'Down_NIS_Buses_Processed New.csv',
  'Bus_Details_Combined_Processed New': 'Bus_Details_Combined_Processed New.csv',
  'Bus_Details_Processed New': 'Bus_Details_Processed New.csv',
  'Chargers_Full_Buses_Processed New': 'Chargers_Full_Buses_Processed New.csv',
  'Low_Charge_Buses_Processed New': 'Low_Charge_Buses_Processed New.csv',
  'Total_Bus_Fleet_Processed New': 'Total_Bus_Fleet_Processed New.csv',
  'Route_Supervisors_Processed New': 'route_supervisors_Processed New.csv',
  'Latest_Entries_Processed New': 'Latest_Entries_Processed New.csv',
  'Personnel_Processed New': 'Personnel_Processed New.csv',
  'Service_Pull_Processed New': 'Service_Pull_Processed New.csv',
  'Operators_Data_Processed New': 'Operators_Data_Processed New.csv'
}

sheet_mapping = {
     'Missing_Blocks_Processed New': 1549254149492612,
     'Missed_Revenue_Processed New': 7547989874659204,
     'Down_NIS_Buses_Processed New': 6066956301979524,
     'Bus_Details_Combined_Processed New': 2190241511198596,
     'Bus_Details_Processed New': 5786194289840004,
     'Chargers_Full_Buses_Processed New': 7299199599071108,
     'Low_Charge_Buses_Processed New': 8520018551590788,      
     'Total_Bus_Fleet_Processed New': 5633817708547972,
     'Route_Supervisors_Processed New': 8014999117057924,
     'Latest_Entries_Processed New': 2729182597435268,
     'Personnel_Processed New': 5058326450622340,
     'Service_Pull_Processed New': 1117212920205188,
     'Operators_Data_Processed New': 5450456696311684
}

dataframes = Processing.read_csv_files(filenames)

for sheet_name, dataframe in dataframes.items():
    sheet_id = sheet_mapping[sheet_name]
    sheet_manager.export_dataframe_to_smartsheet(dataframe, sheet_id)





