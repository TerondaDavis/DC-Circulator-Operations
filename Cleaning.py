import pandas as pd
import re
import urllib
import sqlalchemy
import numpy as np 
from dateutil.parser import parse
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def convert_timestamp(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        return ts.tz_convert('America/New_York').tz_localize(None)
    else:
        return ts.tz_localize('UTC').tz_convert('America/New_York').tz_localize(None)
    
def replace_first_comma(s):
    result = []
    comma_count = 0
    for char in s:
        if char == ',':
            comma_count += 1
            if comma_count % 2 == 0:
                result.append(char)
            else:
                result.append('')
        else:
            result.append(char)
    return ''.join(result)

def rename_columns(df):
    columns_mapping_details = {
        f'Please, provide details for {route} detours': f'Detour Details {route}'
        for route in ['CH-US', 'EM-LP', 'GT-US', 'NMS', 'RS-DP', 'WP-AM']
    }
    columns_mapping_detour = {
        f'Any Detours on {route}?': f'Detour {route}'
        for route in ['CH-US', 'EM-LP', 'GT-US', 'NMS', 'RS-DP', 'WP-AM']
    }
    columns_mapping_supervisors = {
    f'Road Supervisors for {route}': f'Road Supervisors {route}'
    for route in ['CH-US', 'EM-LP', 'GT-US', 'NMS', 'RS-DP', 'WP-AM']
    }
    columns_mapping_available_veh = {
    f'Available Vehicles for {route}': f'Available Vehicles {route}'
    for route in ['CH-US', 'EM-LP', 'GT-US', 'NMS', 'RS-DP', 'WP-AM']
    }
    columns_mapping_available_oper = {
    f'Available Operators for {route}': f'Available Operators {route}'
    for route in ['CH-US', 'EM-LP', 'GT-US', 'NMS', 'RS-DP', 'WP-AM']
    }
    columns_mapping_peak_veh = {
    f'Peak Number of Vehicles on {route}': f'Peak Number of Vehicles {route}'
    for route in ['CH-US', 'EM-LP', 'GT-US', 'NMS', 'RS-DP', 'WP-AM']
    }
    
    df.rename(columns=columns_mapping_details, 
                                       inplace=True) 
    df.rename(columns=columns_mapping_detour, 
                                       inplace=True)
    df.rename(columns=columns_mapping_supervisors, 
                                       inplace=True)
    df.rename(columns=columns_mapping_available_veh, 
                                       inplace=True)
    df.rename(columns=columns_mapping_available_oper, 
                                       inplace=True)
    df.rename(columns=columns_mapping_peak_veh, 
                                       inplace=True)
    return df

def count_names(name_str):
    if pd.isnull(name_str) or not isinstance(name_str, str):
        return 0 
    matches = re.findall(r'\"?\w+, \w+\"?', name_str)
    return len(matches)

def format_details(details):
    if isinstance(details, str):
        details = details.replace('\n•\t', '\n')
        details = details.replace('WESTBOUND:', 'WESTBOUND:\n')
        details = details.replace('EASTBOUND', 'EASTBOUND:\n')
        return details
    else:
        return details
    
def drop_duplicates_within_range(df, min_number, max_number):
    mask = ((df['Bus Number'] >= min_number) & (df['Bus Number'] <= max_number))
    filtered_df = df[mask].drop_duplicates(['Bus Number', 'AM Pull-Out Entry Date'],
                                           keep='first')
    result_df = pd.concat([filtered_df, df[~mask]], 
                          axis=0).sort_index()
    result_df = result_df[result_df['AM Pull-Out Entry Date'].notna()]
    return result_df

def replace_with_option(entry):
    patterns_to_options = {
        r'\bd\b': 'Down',
        r'\bs\b': 'Spare',
        r'\bgt\b': 'GT-US',
        r'\bch\b': 'CH-US',
        r'\bwp\b': 'WP-AM',
        r'\brs\b': 'RS-DP',
        r'\bem\b': 'EM-LP',
        r'\bnms\b': 'NMS',
        r'\bnot in service\b': 'Not in Service'
    }
    entry_lower = entry.lower().strip()
    for pattern, option in patterns_to_options.items():
        if re.fullmatch(pattern, entry_lower):
            return option
    return entry

def capitalize_each_word(s):
    if pd.notna(s):
        entry = s.lower().strip()
        entry = re.sub(r'\b\w', lambda match: match.group(0).upper(), entry)
        return entry
    return s

def convert_time_to_seconds(time_str):
    h, m = map(int, time_str.split(':'))
    return h * 3600 + m * 60 
