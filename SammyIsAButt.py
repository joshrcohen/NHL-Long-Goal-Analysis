import requests
import math
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pyhigh import get_elevation

#SAMMY IS A MASSIVE BUTTOCKS

#I AM ADDING STUFF
#more

def fetch_team_record(team_name, standings):
    """
    Fetch the team's record as of a specific date.
    """
    # Split the provided team name and take the last element (assumed to be the unique part of the team name)
    team_name_key = team_name.split()[-1].lower()
    print(f"TEAM NAME: {team_name_key}")
    for team_info in standings:
        # Split the team name from the standings data and compare the last elements
        team_info_name_key = team_info['teamName']['default'].split()[-1].lower()
        print(f"STANDING TEAM NAME: {team_info_name_key}")
        if team_name_key == team_info_name_key:
            if all(key in team_info for key in ['wins', 'losses', 'otLosses', 'points']):
                record = {
                    'wins': team_info['wins'],
                    'losses': team_info['losses'],
                    'ot_losses': team_info['otLosses'],
                    'points': team_info['points']
                }
                return record
            else:
                print(f"Record keys missing for team: {team_name}")
                return None
    print(f"Team not found: {team_name}")
    return None



def get_arena_elevation(team_name, arenas_info):

    if team_name in arenas_info:
        try:
            lat = float(arenas_info[team_name]['lat'])
            lon = float(arenas_info[team_name]['long'])
            # Construct the URL for the OpenTopoData API
            api_url = f"https://api.opentopodata.org/v1/aster30m?locations={lat},{lon}"
            response = requests.get(api_url).json()

            # Check if the API response is OK and contains the elevation data
            if response['status'] == 'OK' and 'results' in response and len(response['results']) > 0:
                elevation = response['results'][0]['elevation']
                return f"{elevation:.1f} meters"
            else:
                print("Error in API response:", response)
                return None

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


season = 2021
shots_data = []
TOTAL_GAMES = 3

# Fetch arena information
arena_url = "https://raw.githubusercontent.com/nhlscorebot/arenas/master/teams.json"
arenas_info = requests.get(arena_url).json()

# Create a reverse mapping from arena names to team names
arena_to_team_map = {info['arena']: team for team, info in arenas_info.items()}
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


# Iterate over game IDs
for game_id in range(1, TOTAL_GAMES + 1):
    formatted_game_id = f"{season}02{game_id:04d}"
    print(f"Checking game: {game_id}")

    try:
        # Fetch play-by-play data
        pbp_endpoint = f"https://api-web.nhle.com/v1/gamecenter/{formatted_game_id}/play-by-play"
        pbp_response = requests.get(pbp_endpoint)
        game_data = pbp_response.json()
        plays = game_data['plays']
        arena = game_data['venue']['default']

        # Format the game date

        game_datetime = datetime.strptime(game_data['gameDate'], "%Y-%m-%d")
        game_date = game_datetime.strftime("%B %d, %Y")
        standings_date = (game_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"{standings_date}")
        standings_url = f"https://api-web.nhle.com/v1/standings/{standings_date}"
        standings_response = requests.get(standings_url)

        # Create a dictionary for player IDs and names
        players = {player['playerId']: f"{player['firstName']['default']} {player['lastName']['default']}" 
                   for player in game_data['rosterSpots']}

        # Fetch team names
        away_team = game_data['awayTeam']['name']['default']
        home_team = game_data['homeTeam']['name']['default']
        # Assuming you have these variables available from the event data
        home_team_id = game_data['homeTeam']['id']
        

        


        # Generate the play-by-play URL
        pbp_url = f"https://www.nhl.com/scores/htmlreports/{season}{season+1}/PL{formatted_game_id[-6:]}.HTM"
        current_away_score = 0
        current_home_score = 0

        home_team_short = game_data['homeTeam']['name']['default']
        away_team_short = game_data['awayTeam']['name']['default']

        home_team_full = team_name_mapping.get(home_team_short, "Unknown Team")
        away_team_full = team_name_mapping.get(away_team_short, "Unknown Team")

        if home_team_full == "Unknown Team":
            print(f"Could not find full name for team: {home_team_short}")
            continue  # Optionally skip this game if full team name is not found

        if standings_response.status_code == 200:
            standings_data = standings_response.json()['standings']
            home_team_record = fetch_team_record(home_team_full, standings_data)
            away_team_record = fetch_team_record(away_team_full, standings_data)
        else:
            print(f"Failed to fetch standings for date {standings_date}. {away_team_full} v. {home_team_full}")
            continue

        arena_elevation = get_arena_elevation(home_team_full, arenas_info)

        for play in plays:
            type_desc_key = play['typeDescKey'].strip().lower()

            if 'details' in play:
                if 'awayScore' in play['details']:
                    score_away = play['details']['awayScore']
                    score_home = play['details']['homeScore']

                    # Update the score variables
                    current_away_score = score_away
                    current_home_score = score_home
                else:
                    score_away = current_away_score
                    score_home = current_home_score

            if type_desc_key in ['shot-on-goal', 'goal', 'missed-shot'] and 'details' in play:
                if type_desc_key == 'shot-on-goal' or type_desc_key == 'missed-shot':
                    shooter_id = play['details']['shootingPlayerId']
                    scoring_player_id = None  # Set to None for shots
                else:  # type_desc_key == 'goal'
                    shooter_id = None  # Set to None for goals
                    scoring_player_id = play['details']['scoringPlayerId']

                shooter_name = players.get(shooter_id, "Unknown Player")
                scoring_player_name = players.get(scoring_player_id, "Unknown Player")
                xCoord = play['details']['xCoord']
                yCoord = play['details']['yCoord']
                shooting_team_id = play['details']['eventOwnerTeamId']
                home_team_defending_side = play['homeTeamDefendingSide'].lower()

                # Calculate the shot distance using the function
                shot_distance = calculate_shot_distance(xCoord, yCoord, shooting_team_id, home_team_id, home_team_defending_side)

                period = play['period']
                time_in_period = play['timeRemaining']
                minutes, seconds = map(int, time_in_period.split(":"))
                total_seconds_remaining = (minutes * 60) + seconds
                   # and shot_distance >= 90
                if total_seconds_remaining <= 5 and period <= 4:
                    event_description = ""
                    if type_desc_key == 'shot-on-goal':
                        playType = "SHOT"
                        event_description = f"{shooter_name} ({away_team if shooting_team_id == game_data['awayTeam']['id'] else home_team})"
                    elif type_desc_key == 'missed-shot':
                        playType = "MISS"
                        event_description = f"{shooter_name} ({away_team if shooting_team_id == game_data['awayTeam']['id'] else home_team})"
                    else:  # type_desc_key == 'goal'
                        playType = "GOAL"
                        event_description = f"Goal by {scoring_player_name} ({away_team if shooting_team_id == game_data['awayTeam']['id'] else home_team})"
                    print(f"HOME: {home_team_record['wins']}-{home_team_record['losses']}")
                    print(f"AWAY: {away_team_record['wins']}-{away_team_record['losses']}")
                    if home_team_record and away_team_record:
                        teams_formatted = f"{away_team_full} ({away_team_record['wins']}-{away_team_record['losses']}-{away_team_record['ot_losses']}) vs. {home_team_full} ({home_team_record['wins']}-{home_team_record['losses']}-{home_team_record['ot_losses']})"
                    else:
                        teams_formatted = f"{away_team_full} vs. {home_team_full}"

                    shot_info = {
                        "GameID": formatted_game_id,
                        "Arena": arena,
                        "Elevation": f"{arena_elevation} meters" if arena_elevation is not None else "Unknown",
                        "Date": game_date,
                        "Teams": teams_formatted,
                        "Period": period,
                        "Time Left": time_in_period,
                        "Play Type": playType,
                        "Event Description": event_description,
                        "Distance": f"{shot_distance:.2f} feet",
                        "Score": f"{score_away} - {score_home}",
                        "Play-by-Play URL": pbp_url
                    }
                    shots_data.append(shot_info)


    except Exception as e:
        print(f"Error processing game {formatted_game_id}: {e}")

# Create DataFrame and export to Excel
file_name = f'nhl_shots_data_{season}.xlsx'
df = pd.DataFrame(shots_data)
df.to_excel(file_name, index=False)