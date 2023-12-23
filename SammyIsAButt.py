import aiohttp
from aiohttp import TCPConnector
import os
import asyncio
import math
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.lines import Line2D
from matplotlib.animation import FuncAnimation
from concurrent.futures import ThreadPoolExecutor

# Constants
BATCH_SIZE = 1312
BACKGROUND_IMAGE_PATH = './Resources/hockeyRink.png'
DISTANCE_CUTOFF = -1
TIME_CUTOFF = 5
BASE_URL = "https://api-web.nhle.com/v1/gamecenter"
ARENA_URL = "https://raw.githubusercontent.com/nhlscorebot/arenas/master/teams.json"

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
    2020: 868,
    2021: 1312,
    2022: 1312,
}

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

class DataFetcher:
    def __init__(self, session):
        self.session = session

    async def fetch_team_record(self, team_name, standings):
        team_name_key = team_name.split()[-1].lower()
        for team_info in standings:
            team_info_name_key = team_info['teamName']['default'].split()[-1].lower()
            if team_name_key == team_info_name_key:
                if all(key in team_info for key in ['wins', 'losses', 'otLosses', 'points']):
                    return {'wins': team_info['wins'], 'losses': team_info['losses'], 'ot_losses': team_info['otLosses'], 'points': team_info['points']}
        return {'wins': 0, 'losses': 0, 'ot_losses': 0, 'points': 0}

    async def get_arena_elevation(self, team_name, arenas_info):
        if team_name in arenas_info:
            try:
                lat = float(arenas_info[team_name]['lat'])
                lon = float(arenas_info[team_name]['long'])
                api_url = f"https://api.opentopodata.org/v1/aster30m?locations={lat},{lon}"
                async with self.session.get(api_url) as response:
                    response_json = await response.json()
                    if response_json['status'] == 'OK' and 'results' in response_json and len(response_json['results']) > 0:
                        return f"{response_json['results'][0]['elevation']:.1f} meters"
            except Exception as e:
                print(f"Error while fetching elevation for {team_name}: {e}")
                return None
        else:
            print(f"{team_name} not found in arenas_info")
            return None

    async def fetch_game_data(self, game_id):
        pbp_endpoint = f"{BASE_URL}/{game_id}/play-by-play"
        async with self.session.get(pbp_endpoint) as pbp_response:
            if pbp_response.content_type == 'application/json':
                return await pbp_response.json()
            else:
                print(f"Non-JSON response for game {game_id}: {pbp_response.status} {pbp_response.reason}")
                return None

    async def fetch_landing_data(self, game_id):
        landing_endpoint = f"{BASE_URL}/{game_id}/landing"
        async with self.session.get(landing_endpoint) as response:
            if response.content_type == 'application/json':
                return await response.json()
            else:
                print(f"Non-JSON response for game {game_id}: {response.status} {response.reason}")
                return None

    async def fetch_standings_data(self, game_data):
        game_datetime = datetime.strptime(game_data['gameDate'], "%Y-%m-%d")
        standings_date = (game_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
        standings_url = f"https://api-web.nhle.com/v1/standings/{standings_date}"
        async with self.session.get(standings_url) as response:
            standings_response_json = await response.json()
            return standings_response_json['standings']


class DataProcessor:
    @staticmethod
    def calculate_shot_distance(x_coord, y_coord, shooting_team, home_team_id, home_team_defending_side, zone_code):
        if home_team_defending_side is not None:
            if shooting_team == home_team_id:
                target_goal_x = 89 if home_team_defending_side == "left" else -89
            else:
                target_goal_x = -89 if home_team_defending_side == "left" else 89
        else:
            if zone_code == 'O':
                target_goal_x = -89 if x_coord < 0 else 89
            elif zone_code in ['N', 'D']:
                target_goal_x = 89 if x_coord < 0 else -89
            else:
                target_goal_x = -89 if x_coord > 0 else 89

        target_goal_y = 0
        distance = math.sqrt(((x_coord - target_goal_x) ** 2) + ((y_coord - target_goal_y) ** 2))
        return distance

    @staticmethod
    def process_play(play, game_data, players, team_name_mapping, scoring, game_date, arena, season, home_team_record, away_team_record):
        try:
            details = play.get('details', {})
            shooting_team_id = details.get('eventOwnerTeamId')

            if not details or 'xCoord' not in details or 'yCoord' not in details or not shooting_team_id:
                return None

            home_team_defending_side = play.get('homeTeamDefendingSide')
            zone_code = details.get('zoneCode', 'O')
            shot_distance = DataProcessor.calculate_shot_distance(details['xCoord'], details['yCoord'], shooting_team_id, game_data['homeTeam']['id'], home_team_defending_side, zone_code)

            time_remaining = play.get('timeRemaining', "0:0")
            minutes, seconds = map(int, time_remaining.split(":"))
            total_seconds_remaining = minutes * 60 + seconds
            if total_seconds_remaining > TIME_CUTOFF or play['period'] > 4 or shot_distance < DISTANCE_CUTOFF:
                return None

            play_type = "GOAL" if play['typeDescKey'].strip().lower() == 'goal' else "SHOT" if play['typeDescKey'].strip().lower() == 'shot-on-goal' else "MISS"
            player_id = details.get('shootingPlayerId') if play_type != 'GOAL' else details.get('scoringPlayerId')
            player_name = players.get(player_id, "Unknown Player")

            event_description = f"{play_type} by {player_name}"
            if play_type == 'GOAL':
                goals_to_date = details.get('scoringPlayerTotal', 0)
                event_description += f". Goals To Date: {goals_to_date}"

            teams_formatted = f"{game_data['homeTeam']['name']['default']} ({home_team_record['wins']}-{home_team_record['losses']}-{home_team_record['ot_losses']}-{home_team_record['points']}) v. {game_data['awayTeam']['name']['default']} ({away_team_record['wins']}-{away_team_record['losses']}-{away_team_record['ot_losses']}-{away_team_record['points']})"

            return {
                "GameID": game_data["id"],
                "Arena": arena,
                "Date": game_date,
                "Home (w,l,otl,p) v Away (w,l,otl,p)": teams_formatted,
                "Period": play['period'],
                "Time Left": play['timeRemaining'],
                "Play Type": play_type,
                "Shot Type": details.get('shotType', 'Unknown').capitalize(),
                "x_coord": details['xCoord'],
                "y_coord": details['yCoord'],
                "Event Description": event_description,
                "Distance": f"{shot_distance:.2f} feet",
                "Score": f"{details.get('awayScore', 0)} - {details.get('homeScore', 0)}",
                "Play-by-Play URL": f"https://www.nhl.com/scores/htmlreports/{season}{season+1}/PL02{str(game_data['id'])[-4:]}.HTM",
                "Season": season
            }
        except Exception as e:
            print(f"Unhandled error in process_play: {e} - Play: {play}")
            return None

    @staticmethod
    async def process_game(game_id, season, fetcher, team_name_mapping):
        formatted_game_id = f"{season}02{game_id:04d}"
        print(f"Checking game: {game_id} Season: {season}")

        try:
            game_data = await fetcher.fetch_game_data(formatted_game_id)
            landing_data = await fetcher.fetch_landing_data(formatted_game_id)
            standings = await fetcher.fetch_standings_data(game_data)
            if not game_data or not landing_data or not standings:
                return []

            if 'plays' not in game_data:
                return []

            game_date = datetime.strptime(game_data['gameDate'], "%Y-%m-%d").strftime("%B %d, %Y")
            players = {player['playerId']: f"{player['firstName']['default']} {player['lastName']['default']}" for player in game_data.get('rosterSpots', [])}
            arena = game_data['venue']['default']
            scoring = landing_data['summary'].get('scoring', {})

            home_team_record = await fetcher.fetch_team_record(game_data['homeTeam']['name']['default'], standings)
            away_team_record = await fetcher.fetch_team_record(game_data['awayTeam']['name']['default'], standings)

            shots_data = []
            for play in game_data['plays']:
                if 'details' in play and play['typeDescKey'].strip().lower() in ['shot-on-goal', 'goal', 'missed-shot']:
                    shot_data = DataProcessor.process_play(play, game_data, players, team_name_mapping, scoring, game_date, arena, season, home_team_record, away_team_record)
                    if shot_data:
                        shots_data.append(shot_data)

            return shots_data

        except Exception as e:
            print(f"Error processing game {formatted_game_id}: {e}")
            return []


class Visualizer:
    def __init__(self, background_image_path):
        self.background_image_path = background_image_path

    def create_heatmap(self, df, output_path):
        if df.empty:
            print("DataFrame is empty. Cannot create heatmap.")
            return

        plt.figure(figsize=(12, 7))
        img = mpimg.imread(self.background_image_path)
        plt.imshow(img, extent=[-100, 100, -42.5, 42.5])

        sns.kdeplot(
            data=df,
            x='x_coord',
            y='y_coord',
            cmap="Reds",
            fill=True,
            alpha=0.7
        )

        plt.xlim(-100, 100)
        plt.ylim(-42.5, 42.5)
        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        print(f"Heatmap saved as '{output_path}'")

    def create_seasonal_heatmap(self, df, season, output_folder):
        season_df = df[df['Season'] == season]
        if season_df.empty:
            print(f"No data for season {season}. Skipping heatmap generation.")
            return

        heatmap_filename = f'{output_folder}/heatmap_season_{season}.png'
        self.create_heatmap(season_df, heatmap_filename)

    def animate_heatmaps(self, seasons, output_folder):
        fig, ax = plt.subplots(figsize=(10, 7))
        fig.subplots_adjust(top=0.85)
        img = mpimg.imread(self.background_image_path)
        ax.imshow(img, extent=[-100, 100, -42.5, 42.5], aspect='auto')
        ax.axis('off')

        def animate(season):
            ax.clear()
            ax.imshow(img, extent=[-100, 100, -42.5, 42.5], aspect='auto')
            heatmap_path = f'{output_folder}/heatmap_season_{season}.png'

            if os.path.exists(heatmap_path):
                heatmap_img = mpimg.imread(heatmap_path)
                ax.imshow(heatmap_img, extent=[-100, 100, -42.5, 42.5], aspect='auto')
            else:
                print(f"Heatmap for season {season} not found.")

            ax.axis('off')
            plt.title(f"Season {season}", fontsize=16)

        anim = FuncAnimation(fig, animate, frames=seasons, interval=800, repeat_delay=2000)

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        anim.save(f'{output_folder}/seasons_heatmap_animation.gif', writer='pillow', fps=4)
        plt.close()

async def main():
    start_time = time.time()
    all_seasons_shots_data = []

    connector = TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        fetcher = DataFetcher(session)
        visualizer = Visualizer(BACKGROUND_IMAGE_PATH)
        team_name_mapping = get_team_name_mapping()

        for season, total_games in SEASONS_GAMES_MAPPING.items():
            tasks = []
            for game_id in range(1, total_games + 1):
                task = asyncio.ensure_future(DataProcessor.process_game(game_id, season, fetcher, team_name_mapping))
                tasks.append(task)

                if len(tasks) >= BATCH_SIZE:
                    results = await asyncio.gather(*tasks)
                    for game_shots in results:
                        all_seasons_shots_data.extend(game_shots)
                    tasks = []

            if tasks:
                results = await asyncio.gather(*tasks)
                for game_shots in results:
                    all_seasons_shots_data.extend(game_shots)

        df = pd.DataFrame(all_seasons_shots_data)

        output_folder = 'heatmaps'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        for season in SEASONS_GAMES_MAPPING.keys():
            visualizer.create_seasonal_heatmap(df, season, output_folder)

        visualizer.animate_heatmaps(SEASONS_GAMES_MAPPING.keys(), output_folder)

        start_year = min(SEASONS_GAMES_MAPPING.keys())
        end_year = max(SEASONS_GAMES_MAPPING.keys())
        excel_file_name = f'nhl_shots_data_{start_year}_{end_year}.xlsx'
        df.to_excel(excel_file_name, index=False, engine='xlsxwriter')

    print_runtime(start_time)


def print_runtime(start_time):
    total_seconds = int(time.time() - start_time)
    hours, minutes, seconds = total_seconds // 3600, (total_seconds % 3600) // 60, total_seconds % 60
    print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")

if __name__ == "__main__":
    asyncio.run(main())
