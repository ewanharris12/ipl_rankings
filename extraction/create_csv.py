#  Imports
# cricsummary opens the yaml files, something I've made a commit to so its functionality is extended
from cricsummary import Duranz
import yaml
import pandas as pd
import glob
import numpy as np
import math
from datetime import datetime
import os

# Get paths
cwd = os.getcwd()
path_split = cwd.split('/')

root_path = r'/'.join(path_split[:-1])
path = root_path+'/ipl_rankings/ipl_data'
all_files = glob.glob(path + "/*.yaml")

#  extract match info function
def extract_match_info(info):
    dic = {}
    for i in info:
        #print(i)
        if isinstance(info[i],list):
            if len(info[i]) > 1:
                c=0
                if i in ['dates','player_of_match']:
                    dic[i] = info[i][0]
                else:
                    for n in info[i]:
                        c+=1
                        dic[i+'_'+str(c)] = n
                        #print(i+'_'+str(c), n)

            else:
                dic[i] = info[i][0]
                #print(info[i][0])

        elif isinstance(info[i],dict):
            for d in info[i]:
                if i+'_'+d == 'outcome_by':
                    dic['outcome_by'] = list(info[i][d].keys())[0]
                    dic['outcome_margin'] = list(info[i][d].values())[0]
                    
                else:
                    dic[i+'_'+d] = info[i][d]
                #print(d)

        else:
            dic[i] = info[i]
            
    return dic

# Extract data
bb = pd.DataFrame()
mi = pd.DataFrame()
skipped = []

for file in all_files:
    match_id = file.split('/')[-1].replace('.yaml','')
    # % progress bar
    print(round(((all_files.index(file)+1)/len(all_files)*100),1))
    
    try:
        match = Duranz(file)
        _load = open(file,'r')
        _info = match.info

        _innings1 = match.team1_df
        _innings2 = match.team2_df

        _bb = pd.concat([_innings1,_innings2])    

        _bb.insert(0, 'match_id', match_id)

        bb = pd.concat([bb,_bb])


        _match_info = extract_match_info(_info)

        _mi = pd.DataFrame(_match_info, index=[match_id])

        mi = pd.concat([_mi,mi])

    except Exception as e: # work on python 3.x
        #logger.error('Failed to upload to ftp: '+ str(e)):
        print('Skipped ', match_id)
        skipped.append([match_id,str(e)])
        
print('\n ***** DONE *****')

#  match_info clean up
# If a column is entirely blank, remove it
mi.dropna(how='all',axis=1, inplace=True)

# match id was used as the index, pop this out into its own column
mi.reset_index(inplace=True)

mi['dates'] = pd.to_datetime(mi['dates'])

# rename some of the columns
mi.rename(columns={'index':'match_id',
                  'teams_1':'home_team',
                  'teams_2':'away_team',
                  'umpires_1':'umpire_1',
                  'umpires_2':'umpire_2',
                  'outcome_by':'outcome',
                  'outcome_margin':'margin',
                  'outcome_winner':'winner',
                  'outcome_eliminator':'eliminator',
                  'outcome_bowl_out':'bowl_out',
                  'dates':'match_date'}, inplace=True)

mi['match_date'] = pd.to_datetime(mi['match_date'])

# Create some new columns
mi['competition_id'] = 1
mi['competition_name'] = 'IPL'
mi['season'] = mi['match_date'].apply(lambda x: x.year)

# Make neutral vennue a True / False column
mi['neutral_venue'] = np.where(mi['neutral_venue']==1,True,False)

# Get data types correct
mi['match_id'] = mi['match_id'].astype('int')
mi['competition_id'] = mi['competition_id'].astype('int')
mi['season'] = mi['season'].astype('str')
mi['margin'] = mi['margin'].astype('float')

# Fill null cells
mi = mi.where(pd.notnull(mi), None)

# Write to csv
mi.to_csv(root_path+'/ipl_rankings/extraction/mi_ipl.csv', index=False)

# Ball by ball info cleanup

# Split over and ball
bb['over'] = bb['Over_and_ball'].apply(lambda x: str(x).split('.')[0])
bb['ball'] = bb['Over_and_ball'].apply(lambda x: str(x).split('.')[1])

# Set data types
bb['match_id'] = bb['match_id'].astype('int')
bb['over'] = bb['over'].astype('int')
bb['ball'] = bb['ball'].astype('int')

# Remove cricsheet filled values, replace with nulls
bb['Kind_of_wicket'] = np.where(bb['Extra_type']=='-', None, bb['Extra_type'])
bb['Kind_of_wicket'] = np.where(bb['Kind_of_wicket']==0, None, bb['Kind_of_wicket'])
bb['Dismissed_player'] = np.where(bb['Dismissed_player']==0, None, bb['Dismissed_player'])

# rename some of the columns
bb.rename(columns={'Kind_of_wicket':'wicket_type',
                  'Dismissed_player':'player_out',
                  'Extra_type':'extra_type'}, inplace=True)

# Don't want run outs etc to count towards bowling stats
bowling_wickets = ['caught','lbw','bowled','caught and bowled','stumped','hit wicket']
bb['bowling_wicket'] = bb['wicket_type'].isin(bowling_wickets)

# Write to csv
bb.to_csv(root_path+'/ipl_rankings/extraction/bb_ipl.csv', index=False)