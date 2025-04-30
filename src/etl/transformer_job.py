import pandas as pd
import sys
#from awsglue.transforms import *
#from awsglue.utils import getResolvedOptions
#from awsglue.job import Job
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def set_column_data_intorfloat(df_col):
    if df_col.str.contains('.'):
        return df_col.astype(float)
    else:
        return df_col.astype(int)

def check_if_majority_null(df, colname, indicator, threshold=0.5):
    if df:
        for row in df[colname]:
            pass

def transform_data(data):
    raw_data_df = pd.DataFrame(data)
    
    #Processing raw data for cleaning
    processed_data_df = raw_data_df.drop_duplicates()
    column_names = processed_data_df.columns
    
    #Assigning columns their proper datatypes
    for col in column_names:
        if processed_data_df[col].str.isnumeric():
            processed_data_df[col] = processed_data_df[col].apply(set_column_data_intorfloat)

    #Pokemon that are mono-types        
    processed_data_df['Type 2'].fillna('None', inplace=True)
    #Genderless pokemon numeric value
    processed_data_df['gender_male_ratio'].fillna(255, inplace=True)
    
    #Removing columns with majority null values
    for col in column_names:
        col_dtype = processed_data_df[col].dtype
        ind = None
        if col_dtype == int or col_dtype == float:
           ind = pd.NA
           check_if_majority_null(processed_data_df, col, ind)
        else:
            ind = ""
            check_if_majority_null(processed_data_df, col, ind)
    
    return raw_data_df, processed_data_df