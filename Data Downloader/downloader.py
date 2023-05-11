# %% [markdown]
# This notebook downloads summoner IDs for a given rank, division, and region.

# %%
# IMPORTS
import requests
import numpy as np
import time
import pandas as pd
import os
import datetime
from io import TextIOWrapper
from fastparquet import write
from pandas import DataFrame
from riotwatcher import LolWatcher, ApiError
from pynput import keyboard

# %%
API_KEY = os.getenv('RIOT_API_KEY')
EXPECTED_SUMMONERS_OUT = 205
API_KEY = 'RGAPI-614f434b-c4cc-4799-9af2-08ff81844f61'
lol_watcher = LolWatcher(API_KEY)


# %%
def get_summoner_ids(region: str, rank: str, division: str, start_page: int, num_pages=4) -> tuple[DataFrame, bool]:
    """
    Gets random summoner ids from North America from the various ranks (Diamond:Platinum:Gold:Silver:Bronze)
    Returns a pandas dictionary only with summoner ids that have wins + losses >= 20
    Args:
        region (str): The region we want to search in
        rank (str): The rank we want to get from
        division (str): The divions we want to get from
        num_pages (int, optional): An amount of summoner ids to get, a group of 204 are a 'page'. Defaults to 4.

    Returns:
        bool: Whether getting the summoner ids was successful
    """
    print(f"Current rank {rank} {division}")
    df = pd.DataFrame(columns=['leagueId',
                               'queueType',
                               'tier',
                               'rank',
                               'summonerId',
                               'summonerName',
                               'leaguePoints',
                               'wins',
                               'losses',
                               'veteran',
                               'inactive',
                               'freshBlood',
                               'hotStreak'])
    more_summoners = True
    for page_num in range(start_page, start_page+num_pages+1):
        try:
            # attempt to get a response
            response = lol_watcher.league.entries(region, 'RANKED_SOLO_5x5', rank, division, page_num)

            # if we don't get the expected number of summoners, we have probably reached the end of the catalog
            # in another way, there are no more players in the rank and divison to get 
            if len(response) < EXPECTED_SUMMONERS_OUT:
                more_summoners = False
                
            for summoner in response:
                # We only take people with greater than or equal to 20 combined wins and losses
                if summoner['wins'] + summoner['losses'] >= 20:
                    df = pd.concat([df, pd.DataFrame.from_records(summoner, index=[0])], ignore_index=True)
        except ApiError as err:
            print(err)
            print(f'Failed at page number: {page_num}\nFailed at rank and division: {rank} {division}')
            return df, False

    return df, more_summoners

# %%
def write_to_parquet(df: DataFrame) -> str:
    DIRECTORY_PATH = '/Users/ethanshapiro/Repository/MOBA Recommender and Prediction/data/raw_data/'
    date_str = str(datetime.datetime.today()).split()[0]
    file_name = f'summoner_data_{df.loc[0, "tier"]}_{df.loc[0, "rank"]}.parquet'
    file_path = DIRECTORY_PATH + file_name
    # create the parquet if it doesn't exist
    if not os.path.isfile(file_path):
        print(f'Creating {file_name}')
        write(file_path, df)
    else:
        
        print(f'Appending to {file_name}')
        write(file_path, df, append=True)
    return file_path

# %%
# Flag to check if the user wants to stop the code
stop_flag = False

# Function to handle the key press event
def on_press(key):
    global stop_flag
    print(key)
    try:
        if key.char == 'q':
            print('in on press')
            stop_flag = True
            # Stop listening for further key presses
            return False
    except AttributeError:
        pass

# %%
regions = ['na1']
ranks = ['DIAMOND', 'PLATINUM']
tiers = ['I', 'II', 'III', 'IV']

# %%
# get pages 1 to 10,000
page_increments = 20
max_pages = 1_000
first_page = 21

# txt file to save how far we got for each rank
f = open('/Users/ethanshapiro/Repository/MOBA Recommender and Prediction/data/raw_data/progress.txt', 'w+')

# Create a listener for key press events
listener = keyboard.Listener(on_press=on_press)

# Start listening for key presses
listener.start()

for rank in ranks:
    f.write(rank + '\n')
    for tier in tiers:
        f.write(tier + ': ')
        for start_page in range(first_page, max_pages + 1, page_increments):
            # Check if the stop flag is True
            if stop_flag:
                # Close the file
                f.close()
                # Stop the listener
                listener.stop()
                # Exit the loop and stop the code
                break

            # get the summoner id data
            # df, more_data = get_summoner_ids('na1', rank, tier, start_page, page_increments)
            
            # # save to the parquet
            # fp = write_to_parquet(df)
            time.sleep(1)

            # write the current page we completed
            f.write(str(start_page) + ', ')

            # if there isn't more data, we can break
            # if not more_data:
            #     break
        else:
            continue
            
        break
    else:
        continue

    if not stop_flag:
        f.write('\n')
    break

if not stop_flag:
    f.close()



# %%



