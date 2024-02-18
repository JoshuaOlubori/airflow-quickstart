import os
import csv
import requests
from requests.exceptions import RequestException
from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
from time import sleep
from include.global_variables import global_variables as gv

# Remove duplicates using set()
unique_league_ids = set(gv.LEAGUE_IDS)

# Create a top-level folder called "data"
top_level_folder = gv.TEMP_GLOBAL_PATH
os.makedirs(top_level_folder, exist_ok=True)

# Your API request code
url = gv.API_ENDPOINT
headers = {
    "X-RapidAPI-Key": gv.API_KEY,
    "X-RapidAPI-Host": gv.API_HOST
}
# Initialize counters
total_calls = len(unique_league_ids)
current_call = 0



def fetch_data(chosen_season=gv.CHOSEN_SEASON):
    global current_call
    for league_id in unique_league_ids:
        try:
            querystring = {"league": str(league_id), "season": chosen_season}
            response = requests.get(url, headers=headers, params=querystring)

            # Check if the response is successful
            response.raise_for_status()

            data = response.json()['response']

            # Extracting information and preparing the data for CSV
            csv_data = []
            for fixture in data:
                fixture_data = {
                    'date': fixture['fixture']['date'],
                    'season': fixture['league']['season'],
                    'league_name': fixture['league']['name'],
                    'country': fixture['league']['country'],
                    'home_team': fixture['teams']['home']['name'],
                    'home_team_score': fixture['goals']['home'],
                    'away_team_score': fixture['goals']['away'],
                    'away_team': fixture['teams']['away']['name'],
                    'match_status': fixture['fixture']['status']['long']
                }
                csv_data.append(fixture_data)

            # Create a subfolder inside "data" if it doesn't exist
            folder_name = os.path.join(top_level_folder, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
            os.makedirs(folder_name, exist_ok=True)

            # Constructing the CSV file path
            csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
            csv_file_path = os.path.join(folder_name, csv_file_name)

            # Writing data to CSV
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
                            'home_team_score', 'away_team_score', 'away_team', 'match_status']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                # Writing the header
                writer.writeheader()

                # Writing the data
                writer.writerows(csv_data)

            current_call += 1

            gv.task_log.info(
            f"""
              {csv_data[0]['country']} {csv_data[0]['league_name']} called: ({current_call}/{total_calls})

              \nCSV file saved at: {csv_file_path}

            """
        )


        except (NewConnectionError, ConnectTimeoutError) as e:
            gv.task_log.warn(
            f"""
               Connection error in API call for league {league_id}: {e}.
               Ensure your internet connection is stable. Exiting the program.
            """
        )
            return  # Exit the program on connection error

        except RequestException as e:
            gv.task_log.warn(
            f"""
            Error in API call for league {league_id}: {e}
            """
        )

        except Exception as e:
            gv.task_log.warn(
            f"""
           Unexpected error in processing league {league_id}: {e}
            """
        )


        # Sleep for 3 seconds before the next API call
        sleep(3)