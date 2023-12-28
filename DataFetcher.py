import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

BASE_URL = os.getenv('BASE_URL')

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