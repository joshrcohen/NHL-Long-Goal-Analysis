import aiohttp
import asyncio
import math
import pandas as pd
from datetime import datetime, timedelta
import time
import json

TOTAL_GAMES = 1312
season = 2021

async def fetch_team_record(team_name, standings):
    team_name_key = team_name.split()[-1].lower()
    for team_info in standings:
        team_info_name_key = team_info['teamName']['default'].split()[-1].lower()
        if team_name_key == team_info_name_key:
            if all(key in team_info for key in ['wins', 'losses', 'otLosses', 'points']):
                return {'wins': team_info['wins'], 'losses': team_info['losses'], 'ot_losses': team_info['otLosses'], 'points': team_info['points']}
    return {'wins': 0, 'losses': 0, 'ot_losses': 0, 'points': 0}

async def get_arena_elevation(team_name, arenas_info, session):
    if team_name in arenas_info:
        try:
            lat = float(arenas_info[team_name]['lat'])
            lon = float(arenas_info[team_name]['long'])
            api_url = f"https://api.opentopodata.org/v1/aster30m?locations={lat},{lon}"
            async with session.get(api_url) as response:
                response_json = await response.json()
                if response_json['status'] == 'OK' and 'results' in response_json and len(response_json['results']) > 0:
                    return f"{response_json['results'][0]['elevation']:.1f} meters"
        except Exception as e:
            print("Error while fetching elevation:", e)
            return None
    else:
        print(f"{team_name} not found in arenas_info")
        return None

def calculate_shot_distance(x_coord, y_coord, shooting_team, home_team_id, home_team_defending_side):
    # If the shooting team is the home team, they shoot towards the away goal, and vice versa.
    if shooting_team == home_team_id:
        # If home team is shooting, the target goal is opposite to the defending side
        if home_team_defending_side == "left":
            target_goal_x = 89
        else:  # home_team_defending_side == "right"
            target_goal_x = -89
    else:
        # If away team is shooting, the target goal is on the same side as the defending side
        if home_team_defending_side == "left":
            target_goal_x = -89
        else:  # home_team_defending_side == "right"
            target_goal_x = 89

    # The y-coordinate for the goal is always 0 (centered along the width of the rink)
    target_goal_y = 0

    # Calculate the distance to the target goal
    distance = math.sqrt( ( (x_coord - target_goal_x) ** 2) + ( (y_coord - target_goal_y) ** 2) )
    return distance

async def process_game(game_id, season, arenas_info, team_name_mapping, session):
    formatted_game_id = f"{season}02{game_id:04d}"
    shots_data = []
    print(f"Checking game: {game_id}")

    try:
        pbp_endpoint = f"https://api-web.nhle.com/v1/gamecenter/{formatted_game_id}/play-by-play"
        async with session.get(pbp_endpoint) as pbp_response:
            game_data = await pbp_response.json()
            

        # Fetch Landing Data - Contains Scoring Info for each period at landing>summary->scoring->[Period Number 0 = First Period]
        landing_endpoint = f"https://api-web.nhle.com/v1/gamecenter/{formatted_game_id}/landing"
        async with session.get(landing_endpoint) as landing_response:
            landing_data = await landing_response.json()

        summary, scoring = landing_data['summary'], landing_data['summary']['scoring']
        plays, arena = game_data['plays'], game_data['venue']['default']
        game_datetime = datetime.strptime(game_data['gameDate'], "%Y-%m-%d")
        game_date, standings_date = game_datetime.strftime("%B %d, %Y"), (game_datetime - timedelta(days=1)).strftime("%Y-%m-%d")

        standings_url = f"https://api-web.nhle.com/v1/standings/{standings_date}"
        async with session.get(standings_url) as standings_response:
            standings_response_json = await standings_response.json()
            standings_data = standings_response_json['standings']

        home_team_record = await fetch_team_record(team_name_mapping.get(game_data['homeTeam']['name']['default'], "Unknown Team"), standings_data)
        away_team_record = await fetch_team_record(team_name_mapping.get(game_data['awayTeam']['name']['default'], "Unknown Team"), standings_data)

        current_away_score, current_home_score = 0, 0

        # Create a dictionary for player IDs and names
        players = {player['playerId']: f"{player['firstName']['default']} {player['lastName']['default']}" for player in game_data['rosterSpots']}

        for play in plays:
            if 'details' in play and 'awayScore' in play['details'] and 'homeScore' in play['details']:
                current_away_score = play['details']['awayScore']
                current_home_score = play['details']['homeScore']

            if play['typeDescKey'].strip().lower() in ['shot-on-goal', 'goal', 'missed-shot'] and 'details' in play:
                details = play['details']
                shooting_team_id = details['eventOwnerTeamId']
                shot_distance = calculate_shot_distance(details['xCoord'], details['yCoord'], shooting_team_id, game_data['homeTeam']['id'], play['homeTeamDefendingSide'].lower())
                
                if play['timeRemaining']:
                    minutes, seconds = map(int, play['timeRemaining'].split(":"))
                    total_seconds_remaining = (minutes * 60) + seconds

                    if total_seconds_remaining <= 5 and play['period'] <= 4 and shot_distance >= 90:
                        playType = "GOAL" if play['typeDescKey'].strip().lower() == 'goal' else "SHOT" if play['typeDescKey'].strip().lower() == 'shot-on-goal' else "MISS"
                        player_id = details.get('shootingPlayerId') if playType != 'GOAL' else details.get('scoringPlayerId')
                        player_name = players.get(player_id, "Unknown Player")
                        team_name = team_name_mapping.get(game_data['awayTeam']['name']['default'] if shooting_team_id == game_data['awayTeam']['id'] else game_data['homeTeam']['name']['default'], "Unknown Team")
                        
                        event_description = f"{playType} by {player_name} ({team_name})"
                        if play['typeDescKey'].strip().lower() == 'goal':
                            landing_period = int(play['period']) - 1
                            time_in_period_datetime = datetime.strptime(play['timeRemaining'], "%H:%M")
                            goals = scoring[int(f"{landing_period}")]['goals']
                            for goal in goals:
                                # time in period for landing page is total time elapsed, where as in play by play it is time remaining
                                time_in_period_landing = datetime.strptime(goal['timeInPeriod'], "%H:%M")
                                time_in_period_sum = time_in_period_datetime + timedelta(hours=time_in_period_landing.hour, minutes=time_in_period_landing.minute)
                                goalCheck = time_in_period_sum.strftime("%H:%M")
                                if f"{goalCheck}" == f"20:00":
                                    goals_to_date = goal['goalsToDate']
                                    event_description += f". Goals To Date: {goals_to_date}"
                                    break
                                        
                        shot_info = {
                            "GameID": formatted_game_id,
                            "Arena": arena,
                            "Date": game_date,
                            "Teams": f"{away_team_record['wins']}-{away_team_record['losses']}-{away_team_record['ot_losses']} vs. {home_team_record['wins']}-{home_team_record['losses']}-{home_team_record['ot_losses']}",
                            "Period": play['period'],
                            "Time Left": play['timeRemaining'],
                            "Play Type": playType,
                            "Event Description": event_description,
                            "Distance": f"{shot_distance:.2f} feet",
                            "Score": f"{current_away_score} - {current_home_score}",
                            "Play-by-Play URL": f"https://www.nhl.com/scores/htmlreports/{season}{season+1}/PL{formatted_game_id[-6:]}.HTM"
                        }
                        shots_data.append(shot_info)
    except Exception as e:
        print(f"Error processing game {formatted_game_id}: {e}")
    return shots_data



async def main():
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        arena_url = "https://raw.githubusercontent.com/nhlscorebot/arenas/master/teams.json"
        async with session.get(arena_url) as response:
            arenas_info_text = await response.text()
            arenas_info = json.loads(arenas_info_text)

        team_name_mapping = {
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

        tasks = [process_game(game_id, season, arenas_info, team_name_mapping, session) for game_id in range(1, TOTAL_GAMES + 1)]
        all_shots_data = await asyncio.gather(*tasks)

        df = pd.DataFrame([shot for game_shots in all_shots_data for shot in game_shots])
        df.to_excel(f'nhl_shots_data_{season}.xlsx', index=False)

    total_seconds = int(time.time() - start_time)
    hours, minutes, seconds = total_seconds // 3600, (total_seconds % 3600) // 60, total_seconds % 60
    print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")

if __name__ == "__main__":
    asyncio.run(main())
