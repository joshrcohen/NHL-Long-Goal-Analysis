import os
import math
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

TIME_CUTOFF = int(os.getenv('TIME_CUTOFF'))
DISTANCE_CUTOFF = int(os.getenv('DISTANCE_CUTOFF'))

class DataProcessor:
    #calculate_shot_distance
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

    #process_play
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
            
            #just for david rn
            play_type = "GOAL" if play['typeDescKey'].strip().lower() == 'goal' else "MISS"

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
                #"Play-by-Play URL": f"https://www.nhl.com/scores/htmlreports/{season}{season+1}/PL02{str(game_data['id'])[-4:]}.HTM",
                "Season": season
            }
        except Exception as e:
            print(f"Unhandled error in process_play: {e} - Play: {play}")
            return None
    
    #process_game
    @staticmethod
    async def process_game(game_id, season, fetcher, team_name_mapping):
        formatted_game_id = f"{season}02{game_id:04d}"
        print(f"Checking game: {game_id:04d} Season: {season}")

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
