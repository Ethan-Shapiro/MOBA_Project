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
API_KEY = 'RGAPI-ae6b149e-f547-4b4d-b86b-7bb45695d6c7'
lol_watcher = LolWatcher(API_KEY)

# %%
def get_summoner_puuids(region: str, summonerID: str) -> tuple[DataFrame, bool]:
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

    try:
        # attempt to get a response
        response = lol_watcher.summoner.by_id(region, summonerID)

    except ApiError as err:
        print(err)
        print(f'Failed at summonerID: {summonerID}')
        return None, False

    return pd.DataFrame.from_records(response, index=[0]), True

# %%
def write_to_parquet(df: DataFrame, region: str, rank: str, tier: str) -> str:
    DIRECTORY_PATH = f'F://data//puuids//{region}//'
    date_str = str(datetime.datetime.today()).split()[0]
    file_name = f'puuids_{rank}_{tier}.parquet'
    file_path = DIRECTORY_PATH + file_name
    # create the parquet if it doesn't exist
    if not os.path.isfile(file_path):
        print(f'Creating {file_name}')
        write(file_path, df)
    else:
        write(file_path, df, append=True)
    return file_path

# %%
regions = ['na1']
ranks = ['PLATINUM']
tiers = ['III', 'IV']
SUMMONER_DATA_FP = 'C://Repository//MOBA_Project//data//summoner//'
s = []
stop_flag = False
# Read the summonerIDs for the region
num_requested = 0
for region in regions:
    for rank in ranks:
        for tier in tiers:
            # read the summoner dataframe for the given region
            file_name = f'{region}//summoner_data_{rank}_{tier}.parquet'
            complete_fp = SUMMONER_DATA_FP + file_name

            summoners = pd.read_parquet(complete_fp, columns=['summonerId'])
            print(f'Number of {rank} {tier} summonerIds: {summoners.shape[0]}')
            seen_summoners = set()
            for summoner in summoners['summonerId']:
                num_requested += 1
                
                if summoner in seen_summoners:
                    continue

                seen_summoners.add(summoner)
                df, success = get_summoner_puuids(region, summoner)

                if not success:
                    stop_flag = True

                write_to_parquet(df, region, rank, tier)

                if num_requested % 100 == 0:
                    print(f'Finished: {num_requested}')

# %%



