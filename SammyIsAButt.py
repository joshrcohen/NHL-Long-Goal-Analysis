import aiohttp
import asyncio
import math
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import numpy as np
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.lines import Line2D
from concurrent.futures import ThreadPoolExecutor

TOTAL_GAMES = 1312
SEASON = 2021
BACKGROUND_IMAGE_PATH = './Resources/hockeyRink.png'

# Constants for API URLs
BASE_URL = "https://api-web.nhle.com/v1/gamecenter"
ARENA_URL = "https://raw.githubusercontent.com/nhlscorebot/arenas/master/teams.json"

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

def create_heatmap(df, background_image_path):
    # Ensure 'Distance' is a float, not a string
    df['Distance'] = df['Distance'].str.rstrip(' feet').astype(float)
    long_distance_shots = df[df['Distance'] >= 90]

    plt.figure(figsize=(10, 7))
    
    # Load and display the background image
    img = mpimg.imread(background_image_path)
    plt.imshow(img, extent=[-100, 100, -42.5, 42.5])

    # Create the KDE plot with updated parameters
    # Set bw_adjust to higher value to smooth out the plot and reduce clutter
    ax = sns.kdeplot(
        data=long_distance_shots,
        x='x_coord',
        y='y_coord',
        levels=10,  # Fewer contour levels to reduce clutter
        cmap="viridis",  # A sequential colormap with clear progression
        fill=True,
        bw_adjust=1,  # Increase bandwidth to smooth out the noise
        alpha=0.8  # Less transparency for clearer color distinction
    )

    # Filter out low-density regions to handle outliers
    # Set a threshold for density values, below which contours are not drawn
    for collection in ax.collections:
        # Assuming that the contour levels are sorted
        if collection.get_array().max() < 0.0001:  # Adjust threshold as needed
            collection.remove()

    # Improve color bar readability
    colorbar = plt.colorbar(ax.collections[-1])
    colorbar.set_label('Shot Density')
    colorbar.set_alpha(1)
    colorbar._draw_all()

    plt.title('Long Distance Shot Heatmap')
    plt.xlabel('X Coordinate')
    plt.ylabel('Y Coordinate')
    plt.xlim(-100, 100)
    plt.ylim(-42.5, 42.5)

    # Remove axis lines and labels for a cleaner look
    plt.axis('off')

    # Tight layout to ensure the full plot is saved
    plt.tight_layout()

    # Save the heatmap as an image file and close the figure to release memory
    plt.savefig('long_distance_shot_heatmap_improved.png')
    plt.close()

def create_plot(df, background_image_path):
    # Ensure 'Distance' is a float, not a string
    df['Distance'] = df['Distance'].str.rstrip(' feet').astype(float)
    long_distance_shots = df[df['Distance'] >= 90]

    plt.figure(figsize=(10, 7))

    # Load and display the background image
    img = mpimg.imread(background_image_path)
    plt.imshow(img, extent=[-100, 100, -42.5, 42.5])

    # Plot the points of each shot
    plt.scatter(x=long_distance_shots['x_coord'], y=long_distance_shots['y_coord'], 
                alpha=0.6, edgecolors='w', s=20, color='blue')  # Adjust size (s), color, and transparency (alpha) as needed

    plt.title('Shot Points')
    plt.xlabel('X Coordinate')
    plt.ylabel('Y Coordinate')
    plt.xlim(-100, 100)
    plt.ylim(-42.5, 42.5)

    # Remove axis lines and labels for a cleaner look
    plt.axis('off')

    # Tight layout to ensure the full plot is saved
    plt.tight_layout()

    # Save the plot as an image file and close the figure to release memory
    plt.savefig('shot_points_plot.png')
    plt.close()

def create_arrow_plot(df, background_image_path):
    # Ensure 'Distance' is a float, not a string
    df['Distance'] = df['Distance'].str.rstrip(' feet').astype(float)

    plt.figure(figsize=(10, 7))

    # Load and display the background image
    img = mpimg.imread(background_image_path)
    plt.imshow(img, extent=[-100, 100, -42.5, 42.5])

    # Define arrow properties based on shot type
    shot_types = {
        'wrist': {'color': 'blue', 'size': 0.1},
        'backhand': {'color': 'purple', 'size': 0.1},  # Changed from green to purple
        'slap': {'color': 'red', 'size': 0.1},
        'tip-in': {'color': 'orange', 'size': 0.1},
        'snap': {'color': 'purple', 'size': 0.1},     # Changed from green to purple
        'deflected': {'color': 'yellow', 'size': 0.1},
    }

    # Iterate over shots and draw arrows angled towards the goals
    for index, shot in df.iterrows():
        # Determine the direction of the arrow based on the side of the shot
        goal_x = 89 if shot['x_coord'] < 0 else -89
        goal_y = 0
        
        # Calculate the angle of the arrow to point towards the goal
        angle = math.atan2(goal_y - shot['y_coord'], goal_x - shot['x_coord'])
        
        # Define arrow length and width
        arrow_length = 4  # Increased length for better visibility
        arrow_width = 0.1  # Increased width for a bolder arrow

        # Calculate the arrow components using the angle
        dx = arrow_length * math.cos(angle)
        dy = arrow_length * math.sin(angle)

        # Get the shot type, color, and size
        shot_type = shot['Shot Type'].lower()
        properties = shot_types.get(shot_type, {'color': 'green', 'size': 0.1})
        color = properties['color']
        size = properties['size'] * 10  # Scale up the size for visibility

        if shot['Play Type'].lower() == 'goal':
            # If the shot is a goal, draw the arrow in green
            color = 'green'
            arrow_width = arrow_width * 2  # Make goal arrows thicker
            size = size * 1.5  # Make goal arrows larger

        # Draw the arrow
        plt.arrow(
            shot['x_coord'], shot['y_coord'], dx, dy,
            width=arrow_width, head_width=size, head_length=size,
            fc=color, ec=color, length_includes_head=True, clip_on=True
        )

    # Create a custom legend for shot types
    
    legend_elements = [
        Line2D([0], [0], color='blue', lw=4, label='Wrist Shot'),
        Line2D([0], [0], color='purple', lw=4, label='Backhand Shot'),  # Updated color to purple
        Line2D([0], [0], color='red', lw=4, label='Slap Shot'),
        Line2D([0], [0], color='orange', lw=4, label='Tip-In'),
        Line2D([0], [0], color='purple', lw=4, label='Snap Shot'),      # Updated color to purple
        Line2D([0], [0], color='yellow', lw=4, label='Deflected'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='green', markersize=15, 
               markeredgewidth=2, markeredgecolor='green', label='Goal')  # Updated goal representation
    ]
    # Place the legend outside of the plot area
    plt.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, -0.05), 
               fancybox=True, shadow=True, ncol=4)

    plt.title('Shot Arrows')
    plt.xlabel('X Coordinate')
    plt.ylabel('Y Coordinate')
    plt.xlim(-100, 100)
    plt.ylim(-42.5, 42.5)

    # Remove axis lines and labels for a cleaner look
    plt.axis('off')

    # Tight layout to ensure the full plot is saved
    plt.tight_layout()

    # Save the plot as an image file and close the figure to release memory
    plt.savefig('shot_arrows_plot.png')
    plt.close()

async def create_heatmap_async(df, background_image_path):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, create_heatmap, df, background_image_path)

async def create_plot_async(df, background_image_path):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, create_plot, df, background_image_path)

async def create_arrow_plot_async(df, background_image_path):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, create_arrow_plot, df, background_image_path)

async def fetch_arenas_info(session):
    async with session.get(ARENA_URL) as response:
        arenas_info_text = await response.text()
        return json.loads(arenas_info_text)


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

# Process game data
async def process_game(game_id, session, team_name_mapping):
    formatted_game_id = f"{SEASON}02{game_id:04d}"
    shots_data = []
    print(f"Checking game: {game_id}")

    try:
        game_data = await fetch_game_data(session, formatted_game_id)
        landing_data = await fetch_landing_data(session, formatted_game_id)
        standings_data = await fetch_standings_data(session, game_data)

        summary, scoring = landing_data['summary'], landing_data['summary']['scoring']
        plays, arena = game_data['plays'], game_data['venue']['default']
        game_datetime = datetime.strptime(game_data['gameDate'], "%Y-%m-%d")
        game_date = game_datetime.strftime("%B %d, %Y")

        home_team_record = await fetch_team_record(team_name_mapping.get(game_data['homeTeam']['name']['default'], "Unknown Team"), standings_data)
        away_team_record = await fetch_team_record(team_name_mapping.get(game_data['awayTeam']['name']['default'], "Unknown Team"), standings_data)

        current_away_score, current_home_score = 0, 0
        players = {player['playerId']: f"{player['firstName']['default']} {player['lastName']['default']}" for player in game_data['rosterSpots']}

        for play in plays:
            shots_data += process_play(play, game_data, players, team_name_mapping, current_away_score, current_home_score, scoring, game_date, arena)

    except Exception as e:
        print(f"Error processing game {formatted_game_id}: {e}")

    return shots_data

# Fetch game data from API
async def fetch_game_data(session, game_id):
    pbp_endpoint = f"{BASE_URL}/{game_id}/play-by-play"
    async with session.get(pbp_endpoint) as pbp_response:
        return await pbp_response.json()

async def fetch_landing_data(session, game_id):
    landing_endpoint = f"{BASE_URL}/{game_id}/landing"
    async with session.get(landing_endpoint) as response:
        return await response.json()

async def fetch_standings_data(session, game_data):

    game_datetime = datetime.strptime(game_data['gameDate'], "%Y-%m-%d")
    standings_date = (game_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
    standings_url = f"https://api-web.nhle.com/v1/standings/{standings_date}"
    async with session.get(standings_url) as response:
        standings_response_json = await response.json()
        return standings_response_json['standings']

def process_play(play, game_data, players, team_name_mapping, current_away_score, current_home_score, scoring, game_date, arena):
    shots_data = []
    if 'details' in play and 'awayScore' in play['details'] and 'homeScore' in play['details']:
        current_away_score = play['details']['awayScore']
        current_home_score = play['details']['homeScore']

    if play['typeDescKey'].strip().lower() in ['shot-on-goal', 'goal', 'missed-shot'] and 'details' in play:
        details = play['details']
        shooting_team_id = details['eventOwnerTeamId']
        shot_distance = calculate_shot_distance(details['xCoord'], details['yCoord'], shooting_team_id, game_data['homeTeam']['id'], play['homeTeamDefendingSide'].lower())
        
        # Retrieve the shot type
        shot_type = details.get('shotType', 'Unknown').capitalize()

        if play['timeRemaining']:
            minutes, seconds = map(int, play['timeRemaining'].split(":"))
            total_seconds_remaining = (minutes * 60) + seconds

            if total_seconds_remaining <= 5 and play['period'] <= 4 and shot_distance >= 90:
                play_type = "GOAL" if play['typeDescKey'].strip().lower() == 'goal' else "SHOT" if play['typeDescKey'].strip().lower() == 'shot-on-goal' else "MISS"
                player_id = details.get('shootingPlayerId') if play_type != 'GOAL' else details.get('scoringPlayerId')
                player_name = players.get(player_id, "Unknown Player")
                team_name = team_name_mapping.get(game_data['awayTeam']['name']['default'] if shooting_team_id == game_data['awayTeam']['id'] else game_data['homeTeam']['name']['default'], "Unknown Team")
                
                event_description = f"{play_type} by {player_name} ({team_name})"
                if play['typeDescKey'].strip().lower() == 'goal':
                    landing_period = int(play['period']) - 1
                    time_in_period_datetime = datetime.strptime(play['timeRemaining'], "%H:%M")
                    goals = scoring[int(f"{landing_period}")]['goals']
                    for goal in goals:
                        time_in_period_landing = datetime.strptime(goal['timeInPeriod'], "%H:%M")
                        time_in_period_sum = time_in_period_datetime + timedelta(hours=time_in_period_landing.hour, minutes=time_in_period_landing.minute)
                        goal_check = time_in_period_sum.strftime("%H:%M")
                        if f"{goal_check}" == f"20:00":
                            goals_to_date = goal['goalsToDate']
                            event_description += f". Goals To Date: {goals_to_date}"
                            break
                                   
                shot_info = {
                            "GameID": game_data["id"],
                            "Arena": arena,
                            "Date": game_date,
                            "Teams": f"{current_away_score} - {current_home_score}",
                            "Period": play['period'],
                            "Time Left": play['timeRemaining'],
                            "Play Type": play_type,
                            "Shot Type": shot_type,
                            "x_coord": details['xCoord'],  # Add x coordinate
                            "y_coord": details['yCoord'],  # Add y coordinate
                            "Event Description": event_description,
                            "Distance": f"{shot_distance:.2f} feet",
                            "Score": f"{current_away_score} - {current_home_score}",
                            "Play-by-Play URL": f"https://www.nhl.com/scores/htmlreports/{SEASON}{SEASON+1}/PL{game_data['id']}.HTM"
                        }
                shots_data.append(shot_info)
    return shots_data



# Main async function
async def main():
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        arenas_info = await fetch_arenas_info(session)
        team_name_mapping = get_team_name_mapping()

        tasks = [process_game(game_id, session, team_name_mapping) for game_id in range(1, TOTAL_GAMES + 1)]
        all_shots_data = await asyncio.gather(*tasks)

        df = pd.DataFrame([shot for game_shots in all_shots_data for shot in game_shots])

        #await create_heatmap_async(df, background_image_path) #Heatmap
        #df.to_excel(f'nhl_shots_data_{season}.xlsx', index=False) #Excel File
        #await create_plot_async(df, background_image_path) #point plot
        await create_arrow_plot_async(df, BACKGROUND_IMAGE_PATH) #arrow plot

    print_runtime(start_time)

# Print total runtime
def print_runtime(start_time):
    total_seconds = int(time.time() - start_time)
    hours, minutes, seconds = total_seconds // 3600, (total_seconds % 3600) // 60, total_seconds % 60
    print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")


   

if __name__ == "__main__":
    asyncio.run(main())
