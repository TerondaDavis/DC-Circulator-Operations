import pandas as pd
import re
import urllib
import numpy as np 
from dateutil.parser import parse
from datetime import datetime, timedelta
from Cleaning import convert_timestamp

def get_latest_entries_pullout_delivery(df):
    Pullout_Delivery_Tracking = df[[
        'Last Modified Time',
        'Service Date',
        'Report Time',
        "Respondent's Name"]]
    Pullout_Delivery_Tracking['Last Modified Time'] = Pullout_Delivery_Tracking['Last Modified Time'].apply(convert_timestamp)
    Pullout_Delivery_Tracking['Service Date'] = pd.to_datetime(Pullout_Delivery_Tracking['Service Date'])
    Latest_Entries_Pullout_Delivery = Pullout_Delivery_Tracking[Pullout_Delivery_Tracking['Report Time'].notna()].groupby('Report Time').apply(lambda x: x.nlargest(1, 'Last Modified Time'))
    Latest_Entries_Pullout_Delivery = Latest_Entries_Pullout_Delivery.drop('Report Time', axis=1).reset_index()
    Latest_Entries_Pullout_Delivery = Latest_Entries_Pullout_Delivery.drop('level_1',axis=1).reset_index(drop=True)
    return Latest_Entries_Pullout_Delivery
    
def latest_entry(df, column):
    name_column = [col for col in df.columns if 'Name' in col]
    if name_column:
        name_column = name_column[0] 
    else:
        raise ValueError("No column containing 'Name' found.")
    df["Today's Date"] = df["Today's Date"].apply(convert_timestamp)
    df = df.sort_values(by=column, ascending=False)
    df = df.iloc[0]
    df = {
        "Last Modified Time": df["Today's Date"],
        "Service Date": df[column],
        "Respondent's Name": df[name_column]
    }
    return pd.DataFrame([df])

def flag_late(row):
    time = row['Last Modified Time'].time()
    date = row['Last Modified Time'].date()
    service_date = row['Service Date']
    day_of_week = row['Last Modified Time'].weekday()
    is_weekend = day_of_week >= 5
    if row['Report Time'] == 'Pull-out AM (6 AM-7 AM)':
        limit = pd.Timestamp('07:30').time() if not is_weekend else pd.Timestamp('09:00').time()
    elif row['Report Time'] == 'AM Service Delivery (7 AM-9 AM)':
        limit = pd.Timestamp('09:30').time() if not is_weekend else pd.Timestamp('10:30').time()
    elif row['Report Time'] == 'PM Service Delivery (3 PM-5 PM)':
        limit = pd.Timestamp('17:30').time()
    elif row['Report Time'] == 'Night Service Delivery (9 PM onwards)':
        limit = pd.Timestamp('21:30').time()
    elif row['Report Time'] == 'Bus Details':
        limit = pd.Timestamp('07:30').time() if not is_weekend else pd.Timestamp('09:00').time()
    elif row['Report Time'] == 'Operations':
        limit = pd.Timestamp('12:00').time()
        if date > service_date + pd.Timedelta(days=1) or (date == service_date and time > limit):
            return 'Late'
    else:
        return 'N/A' 
    if date > service_date or (date == service_date and time > limit):
        return 'Late'
    return 'On Time'

def reshape_personnel(df):
    base_cols = ['Created','Service Date', 'Report Time', 'OCC']
    measures = ['Dispatchers']
    garages = ['17th St', 'Hains Point', 'South Capitol']
    reshaped_data = []
    for measure in measures:
        for garage in garages:
            col_name = f"{measure} - {garage}"
            temp_df = df[base_cols + [col_name]].copy()
            temp_df['Garage'] = garage
            temp_df['Measure'] = measure
            temp_df.rename(columns={col_name: 'Value'}, inplace=True)
            reshaped_data.append(temp_df)
    reshaped_df = pd.concat(reshaped_data)
    reshaped_df['Service Date'] = pd.to_datetime(reshaped_df['Service Date'])
    final_df = pd.pivot_table(reshaped_df, index=['Created', 
                                                  'Service Date', 
                                                  'Report Time', 
                                                  'OCC',
                                                  'Garage'],
                              columns='Measure', 
                              values='Value', 
                              aggfunc='first').reset_index()
    # Flatten the columns
    final_df.columns = [col if col else final_df.columns.name for col in final_df.columns]
    final_df.columns.name = None
    return final_df

def reshape_service_pull(df):
    base_cols = ['Last Modified Time','Service Date', 'Report Time']
    measures = ['Road Supervisors',
                'Peak Number of Vehicles', 
                'Available Vehicles', 
                'Available Operators',
                'Detour',
                'Detour Details']
    routes = ['CH-US', 'GT-US', 'WP-AM', 'RS-DP', 'EM-LP', 'NMS']
    reshaped_data = []
    for measure in measures:
        for route in routes:
            col_name = f"{measure} {route}"
            temp_df = df[base_cols + [col_name]].copy()
            temp_df['Route'] = route
            temp_df['Measure'] = measure
            temp_df.rename(columns={col_name: 'Value'}, inplace=True)
            reshaped_data.append(temp_df)
    reshaped_df = pd.concat(reshaped_data)
    reshaped_df['Service Date'] = pd.to_datetime(reshaped_df['Service Date'])
    reshaped_df['Day of Week'] = reshaped_df['Service Date'].dt.day_name()
    weekdays_mapping = {
        'Monday': 'Monday-Thursday',
        'Tuesday': 'Monday-Thursday',
        'Wednesday': 'Monday-Thursday',
        'Thursday': 'Monday-Thursday',
        'Friday': 'Friday',
        'Saturday': 'Saturday',
        'Sunday': 'Sunday'
    }
    reshaped_df['Service Week'] = reshaped_df['Day of Week'].map(weekdays_mapping)
    reshaped_df.drop(columns=['Day of Week'], 
                     inplace=True)
    final_df = pd.pivot_table(reshaped_df, index=['Last Modified Time', 
                                                  'Service Date', 
                                                  'Report Time', 
                                                  'Route', 
                                                  'Service Week'],
                              columns='Measure', 
                              values='Value', 
                              aggfunc='first').reset_index()
    # Flatten the columns
    final_df.columns = [col if col else final_df.columns.name for col in final_df.columns]
    final_df.columns.name = None
    return final_df

def update_operations_report(df, column_name, report_time):
    pull_out_values = df[df['Report Time'] == report_time].groupby(['Service Date', 'Route'])[column_name].first().reset_index()
    pull_out_values.rename(columns={column_name: f'{report_time}_{column_name}'}, inplace=True)
    df = df.merge(pull_out_values, on=['Service Date','Route'], how='left')
    condition = df['Report Time'] == 'Operations Report'
    df.loc[condition, column_name] = df.loc[condition, f'{report_time}_{column_name}']
    df.drop(columns=[f'{report_time}_{column_name}'], inplace=True)
    return df

def proportion(df,x, y):
    if (df[y] == 0).any():
        return None
    return df[x]/df[y]

def delimiting_multiple_down_reasons(df, report):
    split_columns = df['Reason for Down Service'].str.split(',', expand=True)
    split_columns.columns = [f'{report} Down Bus Reason 1'] + [f'{report} Down Bus Reason {i+2}' for i in range(split_columns.shape[1] - 1)]
    df = pd.concat([df, split_columns], axis=1)
    df = df.drop(columns='Reason for Down Service')
    return df

def replace_other_with_reason(df):
    columns_with_down = [col for col in df.columns if 'Down' in col]
    for col in columns_with_down:
        df[col] = df.apply(lambda row: row['Specify Other Reason Here'] if row[col] == 'Other' else row[col], 
                           axis=1)
    return df

def process_time_columns(df, col1, col2, report):
    def convert_time(time_str):
        hh, mm = map(int, time_str.split(':'))
        hh = hh % 24  
        return f"{hh:02d}:{mm:02d}"
    df[col1] = df[col1].apply(convert_time)
    df[col2] = df[col2].apply(convert_time)
    df[col1] = pd.to_datetime(df[col1], format='%H:%M')
    df[col2] = pd.to_datetime(df[col2], format='%H:%M')
    mask = df[col2] < df[col1]
    df.loc[mask, col2] += pd.Timedelta(days=1)
    time_diff = df[col2] - df[col1]
    df[f'{report} Duration'] = time_diff.astype(str).str.split().str[-1]
    df[f'{report} Duration Minutes'] = time_diff.dt.total_seconds() / 60
    df[col1] = df[col1].dt.time
    df[col2] = df[col2].dt.time
    return df

def save_dataframes_to_csv(dataframes):
    for name, df in dataframes.items():
        # Replace NaN values with an empty string
        #df.fillna("", inplace=True)
        file_path = f"Data/{name}.csv"
        df.to_csv(file_path, index=False)
        
def read_csv_files(filenames):
    dataframes = {
#         'Missing_Blocks_Processed New': pd.read_csv(filenames['Missing_Blocks_Processed New']),
#         'Missed_Revenue_Processed New': pd.read_csv(filenames['Missed_Revenue_Processed New']),
#         'Down_NIS_Buses_Processed New': pd.read_csv(filenames['Down_NIS_Buses_Processed New']),
#         'Bus_Details_Combined_Processed New': pd.read_csv(filenames['Bus_Details_Combined_Processed New']),
#         'Bus_Details_Processed New': pd.read_csv(filenames['Bus_Details_Processed New']),
#         'Chargers_Full_Buses_Processed New': pd.read_csv(filenames['Chargers_Full_Buses_Processed New']),
#         'Low_Charge_Buses_Processed New': pd.read_csv(filenames['Low_Charge_Buses_Processed New']),
#         'Total_Bus_Fleet_Processed New': pd.read_csv(filenames['Total_Bus_Fleet_Processed New']),
#         'Route_Supervisors_Processed New': pd.read_csv(filenames['Route_Supervisors_Processed New']),
        'Latest_Entries_Processed New': pd.read_csv(f"Data/{filenames['Latest_Entries_Processed New']}"),
#         'Personnel_Processed New': pd.read_csv(filenames['Personnel_Processed New']),
#         'Service_Pull_Processed New': pd.read_csv(filenames['Service_Pull_Processed New']),
#         'Operators_Data_Processed New': pd.read_csv(filenames['Operators_Data_Processed New'])
    }
    return dataframes

