import pandas as pd
import random
import math

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#Variables for new columns transformation
#Multiply max HP to become curr HP
hp_decr = [1, 0.5, 0.33, 0]

#Using masterball makes instant catch
#Random number N
pokeballs = [['poke_ball',255], ['great_ball',155], ['other_ball',150], ['master_ball',None]]

def compare_to_M(pkmn_df, ball_int):
    if pkmn_df['curr_HP']== 0 or ball_int == 0:
        f_val = 0
    else:
        f_val = math.floor((pkmn_df['HP']*255*4)/(pkmn_df['curr_HP']*ball_int))
    return f_val
 
#if any issue with gen2 and above data this may be the root of it
def prob_of_status(stat, stat_i, pkmn_df):
    if stat == 'FRZ/SLP': #8
        max_damage = 4 * stat_i
        frz_slp = (pkmn_df['ice_weakness'], pkmn_df['psychic_weakness'])
        attk = random.choice(frz_slp)
        sp = attk * stat_i
        if sp <= max_damage/4: #0-25
            return 0 if pkmn_df['generation'] == 1 else 1
        elif sp > max_damage/4 and sp <= max_damage/2: #26-50
            return random.choice([stat_i, 0])
        elif sp > max_damage/2 and sp <= (4*max_damage)/3: #51-75
            return random.choice([stat_i, 0])
        else: #76-100
            return stat_i
    elif stat == 'PAR/BRN/PSN': #6
        max_damage = 4 * stat_i
        par_brn_psn = (pkmn_df['normal_weakness'], pkmn_df['ghost_weakness'], pkmn_df['electric_weakness'], pkmn_df['fire_weakness'], pkmn_df['poison_weakness'])
        attk = random.choice(par_brn_psn)
        sp = attk * stat_i
        if sp <= max_damage/4: #0-25
            return 0 if pkmn_df['generation'] == 1 else 1
        elif sp > max_damage/4 and sp <= max_damage/2: #26-50
            return random.choice([stat_i, 0])
        elif sp > max_damage/2 and sp <= (4*max_damage)/3: #51-75
            return random.choice([stat_i, 0])
        else: #76-100
            return stat_i
    return 0 if pkmn_df['generation'] == 1 else 1
        
def approx_capture_probabilitygen1(pokemon_df, ball):
    status_condition_tuple = (['PAR/BRN/PSN',12], ['FRZ/SLP',25], ('otherwise',0))
    status = random.choice(status_condition_tuple)
    status_str, status_int = status[0], prob_of_status(status[0], status[1], pokemon_df)
    #Cases where by default, or after certain conditions pass, the probability of capture is certain
    cp = 1
    
    #master ball always wins, everything before else is approx
    if ball[0] == 'master_ball':
        return status_str, status_int, cp 
    elif ball[0] == 'poke_ball' and pokemon_df['capturing_rate'] <= 230:
        cp = status_int/(ball[1]+1) + (((pokemon_df['capturing_rate']+1)/(ball[1]+1)) * ((compare_to_M(pokemon_df, ball[1])+1)/256))
        return status_str, status_int, cp
    elif ball[0] == 'great_ball' and pokemon_df['capturing_rate'] <= 175:
        cp = status_int/(ball[1]+1) + (((pokemon_df['capturing_rate']+1)/(ball[1]+1)) * ((compare_to_M(pokemon_df, ball[1])+1)/256))
        return status_str, status_int, cp
    elif ball[0] == 'other_ball' and pokemon_df['capturing_rate'] <= 125:
        cp = status_int/(ball[1]+1) + (((pokemon_df['capturing_rate']+1)/(ball[1]+1)) * ((compare_to_M(pokemon_df, ball[1])+1)/256))
        return status_str, status_int, cp
    else:
        if status_str == 'PAR/BRN/PSN' and ball[1] < status_int:
            return status_str, status_int, cp
        if status_str == 'FRZ/SLP' and ball[1] < status_int:
            return status_str, status_int, cp
        if status_str == 'otherwise':
            comparison = ball[1] - status_int
            if comparison < pokemon_df['capturing_rate']:
                big_m = random.randint(0, 255)
                #greatball is being used: 8
                if ball[0] == 'great_ball':
                    ball[1] = 8
                    f = compare_to_M(pokemon_df,ball[1])
                    if f >= big_m:
                        #if curr HP is 1/2 max HP then def caught
                        return status_str, status_int, cp
                else:
                    #any other ball is being used: 12
                    ball[1] = 12
                    f = compare_to_M(pokemon_df,ball[1])
                    if f >= big_m:
                        return status_str, status_int, cp
    cp = 0
    return status_str, status_int, cp

def approx_capture_probabilitygen2onwards(pokemon_df, ball):
    status_condition_tuple = (['PAR/BRN/PSN',1.5], ['FRZ/SLP',2], ('otherwise',1))
    status = random.choice(status_condition_tuple)
    status_str, status_int = status[0], prob_of_status(status[0], status[1], pokemon_df)
    #Cases where by default, or after certain conditions pass, the probability of capture is certain
    cp = 255
    
    if ball[0] == 'master_ball':
        return status_str, status_int, cp
    elif ball[0] == 'poke_ball':
        if pokemon_df['curr_HP'] == pokemon_df['HP'] and status_str == 'otherwise':
            #pokemon is full health and no status condition
            a = pokemon_df['capturing_rate']/3
            return status_str, status_int, a
        else:
            a = ((3*pokemon_df['HP'] - 2*pokemon_df['curr_HP'])/(3*pokemon_df['HP']))*pokemon_df['capturing_rate']*ball[1]*status_int
            if a >= 255:
                return status_str, status_int, a
    elif ball[0] == 'great_ball':
        a = ((3*pokemon_df['HP'] - 2*pokemon_df['curr_HP'])/(3*pokemon_df['HP']))*pokemon_df['capturing_rate']*ball[1]*status_int
        if a >= 255:
            return status_str, status_int, a
    elif ball[0] == 'other_ball':
        a = ((3*pokemon_df['HP'] - 2*pokemon_df['curr_HP'])/(3*pokemon_df['HP']))*pokemon_df['capturing_rate']*ball[1]*status_int
        if a >= 255:
            return status_str, status_int, a
    cp = random.randint(0, cp-1)  
    return status_str, status_int, cp

def check_if_majority_novalue(df, colname, indicator, threshold=0.5):
    sum_of_no_val = 0
    if indicator == None:
        sum_of_no_val = df[colname].isna().sum() 
    elif indicator == '':
        sum_of_no_val = df[colname].apply(lambda x: x == '').sum()
        
    threshold_amount = len(df) * threshold
    
    if sum_of_no_val > threshold_amount:
        return df.drop(colname, axis=1)
    return df

def transform_data(processing_data_df):
    #Processing raw data for cleaning
    #Dropping duplicates from df
    if 'Name' in processing_data_df.columns or 'Type 1' in processing_data_df.columns:
        processing_data_df = processing_data_df.rename(columns={'Name': 'Pokemon_Name', 'Sp. Attack': 'Sp_Attack', 'Sp. Defense': 'Sp_Defense', 'Type 1': 'Type_1', 'Type 2': 'Type_2', 'gen': 'generation'})
    
    for col in processing_data_df.columns:
        if processing_data_df[col].apply(lambda x: isinstance(x, list)).any():
            processing_data_df[col] = processing_data_df[col].apply(str)
        
    processing_data_df = processing_data_df.drop_duplicates()
    column_names = processing_data_df.columns
    print(column_names)
    
    #Dropping columns with missing Pokemon names and adding ID values
    for index, row in processing_data_df.iterrows():
        if 'Pokemon_Name' in column_names and pd.isnull(row['Pokemon_Name']):
            processing_data_df = processing_data_df.drop(index)
        elif 'ID' in column_names and pd.isnull(row['ID']):
            processing_data_df.at[index, 'ID'] = len(processing_data_df) + 1
            
    #Assigning columns their proper datatypes
    for index, row in processing_data_df.iterrows():
        for col in column_names:
            try:
                processing_data_df[col] = pd.to_numeric(processing_data_df[col])
            except Exception as e:
                print(f"Column contains strings and should be of type string: {e}")
            
    #Pokemon that are mono-types 
    if 'Type_2' in column_names:       
        processing_data_df['Type_2'].fillna('None')
        
    #Genderless pokemon numeric value
    if 'gender_male_ratio' in column_names:
        processing_data_df['gender_male_ratio'].fillna(255).astype(int)
    
    #Removing columns with majority null values
    for col in column_names:
        print(col)
        print(type(processing_data_df[col]))
        col_dtype = processing_data_df[col].dtype
        ind = None
        if col_dtype == int or col_dtype == float:
            processing_data_df = check_if_majority_novalue(processing_data_df, col, ind)
        else:
            ind = ""
            processing_data_df = check_if_majority_novalue(processing_data_df, col, ind)
    
    new_ball_cols = ['curr_HP', 'curr_status', 'curr_status_value', 'pokeball_capture', 'greatball_capture', 'other_balls_capture', 'masterball_capture']
    for col in new_ball_cols:
        processing_data_df[col] = None
    
    processing_data_df['curr_HP'] = processing_data_df.apply(lambda row: random.choice(hp_decr) * row['HP'], axis=1)
    for index, row in processing_data_df.iterrows():
        if row['generation'] == 1:
            if processing_data_df.loc[index,'pokeball_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen1(row, pokeballs[0])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'pokeball_capture'] = ball_capprob
            if processing_data_df.loc[index,'greatball_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen1(row, pokeballs[1])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'greatball_capture'] = ball_capprob
            if processing_data_df.loc[index,'other_balls_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen1(row, pokeballs[2])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'other_balls_capture'] = ball_capprob
            if processing_data_df.loc[index,'masterball_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen1(row, pokeballs[3])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'masterball_capture'] = ball_capprob
        else:
            if processing_data_df.loc[index,'pokeball_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen2onwards(row, pokeballs[0])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'pokeball_capture'] = ball_capprob
            if processing_data_df.loc[index,'greatball_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen2onwards(row, pokeballs[1])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'greatball_capture'] = ball_capprob
            if processing_data_df.loc[index,'other_balls_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen2onwards(row, pokeballs[2])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'other_balls_capture'] = ball_capprob
            if processing_data_df.loc[index,'masterball_capture'] is None:
                curr_stat, curr_stat_val, ball_capprob = approx_capture_probabilitygen2onwards(row, pokeballs[3])
                processing_data_df.loc[index, 'curr_status'] = curr_stat
                processing_data_df.loc[index, 'curr_status_value'] = curr_stat_val
                processing_data_df.loc[index, 'masterball_capture'] = ball_capprob
            
    print(processing_data_df) 
    
    return processing_data_df