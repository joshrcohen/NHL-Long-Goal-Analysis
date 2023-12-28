import aiohttp
from aiohttp import TCPConnector
import os
import asyncio
import pandas as pd
import time
import json
import numpy as np
#import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.lines import Line2D
from matplotlib.animation import FuncAnimation
import matplotlib.patches as mpatches
from concurrent.futures import ThreadPoolExecutor
import DataFetcher as fetch
import DataProcessor as dp
import Visualizer as viz

import os
from dotenv import load_dotenv
load_dotenv()

BACKGROUND_IMAGE_PATH = os.getenv('BACKGROUND_IMAGE_PATH')
BATCH_SIZE = int(os.getenv('BATCH_SIZE'))

# Season games mapping
SEASONS_GAMES_MAPPING = {
    # Add entries for each season with the corresponding number of games 
    #2009: 1230,
    #2010: 1230,
    #2011: 1230,
    #2012: 720,
    #2013: 1230,
    #2014: 1230,
    #2015: 1230,
    #2016: 1230,
    #2017: 1230,
    #2018: 1230,
    #2019: 1082,
    #2020: 868,
    #2021: 1312,
    2022: 1312,
}

#get_team_name_mapping
def get_team_name_mapping():
    """
    Provides a mapping from team abbreviations to full team names.

    Returns:
        dict: A dictionary mapping team abbreviations to full names.
    """
    return {
            "Predators": "Nashville Predators",
            "Senators": "Ottawa Senators",
            "Stars": "Dallas Stars",
            "Panthers": "Florida Panthers",
            "Canadiens": "Montreal Canadiens",
            "Maple Leafs": "Toronto Maple Leafs",
            "Sabres": "Buffalo Sabres",
            "Penguins": "Pittsburgh Penguins",
            "Coyotes": "Phoenix Coyotes",
            "Jets": "Winnipeg Jets",
            "Ducks": "Anaheim Ducks",
            "Red Wings": "Detroit Red Wings",
            "Islanders": "New York Islanders",
            "Avalanche": "Colorado Avalanche",
            "Blue Jackets": "Columbus Blue Jackets",
            "Oilers": "Edmonton Oilers",
            "Hurricanes": "Carolina Hurricanes",
            "Canucks": "Vancouver Canucks",
            "Flames": "Calgary Flames",
            "Sharks": "San Jose Sharks",
            "Devils": "New Jersey Devils",
            "Blues": "St. Louis Blues",
            "Lightning": "Tampa Bay Lightning",
            "Kings": "Los Angeles Kings",
            "Rangers": "New York Rangers",
            "Blackhawks": "Chicago Blackhawks",
            "Wild": "Minnesota Wild",
            "Flyers": "Philadelphia Flyers",
            "Capitals": "Washington Capitals",
            "Bruins": "Boston Bruins",
            "Golden Knights": "Las Vegas Golden Knights",
            "Kraken": "Seattle Kraken"
        }

#print_runtime
def print_runtime(start_time):
    total_seconds = int(time.time() - start_time)
    hours, minutes, seconds = total_seconds // 3600, (total_seconds % 3600) // 60, total_seconds % 60
    print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")

#main
async def main():
    start_time = time.time()
    all_seasons_shots_data = []

    connector = TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        fetcher = fetch.DataFetcher(session)
        visualizer = viz.Visualizer(BACKGROUND_IMAGE_PATH)
        team_name_mapping = get_team_name_mapping()

        for season, total_games in SEASONS_GAMES_MAPPING.items():
            for game_batch_start in range(1, total_games + 1, BATCH_SIZE):
                game_batch_end = min(game_batch_start + BATCH_SIZE, total_games + 1)
                tasks = [asyncio.ensure_future(dp.DataProcessor.process_game(game_id, season, fetcher, team_name_mapping))
                         for game_id in range(game_batch_start, game_batch_end)]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        print(f"Error in processing game: {result}")
                    else:
                        all_seasons_shots_data.extend(result)

        df = pd.DataFrame(all_seasons_shots_data)

        output_folder = 'heatmaps'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        #for season in SEASONS_GAMES_MAPPING.keys():
            #visualizer.create_seasonal_heatmap(df, season, output_folder)

        #visualizer.animate_heatmaps(SEASONS_GAMES_MAPPING.keys(), output_folder)

        start_year = min(SEASONS_GAMES_MAPPING.keys())
        end_year = max(SEASONS_GAMES_MAPPING.keys())
        excel_file_name = f'nhl_shots_data_{start_year}_{end_year}.xlsx'
        df.to_excel(excel_file_name, index=False, engine='xlsxwriter')

    print_runtime(start_time)

if __name__ == "__main__":
    asyncio.run(main())
