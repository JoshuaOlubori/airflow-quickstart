import os
from datetime import datetime
import pandas as pd

pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', None)
pd.reset_option('display.max_columns')

from include.global_variables import global_variables as gv

# Top-level folder containing subfolders with CSV files
#top_level_folder = "data"



def apply_filtering_logic(df):

    def filter_fixtures_today(df):
                # Get the current date in UTC format
                current_date_utc = pd.to_datetime(datetime.utcnow())

                # Extract the date part from the 'Date' column in UTC format
                df['date'] = pd.to_datetime(
                    df['date']).dt.date
                df['date'] = pd.to_datetime(
                    df['date']).dt.date

                # 
                df.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_all_days_across_71_leagues.csv"))
                gv.task_log.info("Saved all fixtures successfully \n")
                # Filter DataFrames based on the current date
                filtered_df_today = df[df['date'] == current_date_utc.date(
                )]
                filtered_df_today = filtered_df_today.drop_duplicates()
                
                filtered_df_today.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_today_across_71_leagues.csv"))
                gv.task_log.info("Save successful. Showing today's fixtures. \n")

                gv.task_log.info(filtered_df_today.drop(columns=["season", "country", "date"], errors='ignore'))
                #return filtered_df_today

#     # Initialize an empty list to store DataFrames
#     dfs = []

# # Initialize final_df to None
#     df = None


#         # Concatenate all DataFrames into one
#         # df = pd.concat(dfs, ignore_index=True)
#     df = pd.read_csv(gv.FIXTURES_DATA_PATH, "all_fixtures_combined.csv")
    df = df.drop_duplicates()
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df['home_team_score'] = pd.to_numeric(
        df['home_team_score'], errors='coerce').astype('Int64')
    df['away_team_score'] = pd.to_numeric(
        df['away_team_score'], errors='coerce').astype('Int64')
    # df['date'] = pd.to_datetime(df['date'], utc=True)
    # Display the first few rows of the resulting DataFrame
    gv.task_log.info("Import successful for all files.\n")
    gv.task_log.info(df.info())
    gv.task_log.info("\n")
    filter_fixtures_today(df)
    gv.task_log.info("\n")


    # Now final_df is accessible outside the try block
    if df is not None:
        try:
            # Do something with final_df
            gv.task_log.info("Applying filtering logic...  This would take some time.\n")
            df1 = df[df["match_status"] == "Match Finished"]

            # Initialize lists to store data
            dates_both_conditions = []
            home_teams_both_conditions = []
            home_scores_both_conditions = []
            away_scores_both_conditions = []
            away_teams_both_conditions = []

            # dates_first_condition = []
            # home_teams_first_condition = []
            # home_scores_first_condition = []
            # away_scores_first_condition = []
            # away_teams_first_condition = []

            def team_won_3_games_or_more(team, g):
                win_count = 0
                for index, row in g.iterrows():
                    if row["home_team"] != team:
                        # Swap home and away team names and scores
                        g.loc[index, "home_team"] = row["away_team"]
                        g.loc[index, "away_team"] = row["home_team"]

                        g.loc[index, "home_team_score"] = row["away_team_score"]
                        g.loc[index, "away_team_score"] = row["home_team_score"]

                win_count = 0
                for index, row in g.iterrows():
                    if g.loc[index, "home_team_score"] > g.loc[index, "away_team_score"]:
                        win_count += 1
                if win_count >= 3:
                    return True

            # Find all match containers
            match_containers = df.to_dict('records')

            # Loop through each match container
            for match in match_containers:
                home_team = match['home_team']
                away_team = match['away_team']


                # Get the last 5 fixtures for both home and away teams
                last_5_home_team_fixtures = df1[(df1['home_team'] == home_team) | (
                    df1['away_team'] == home_team)].sort_values(by='date', ascending=False).head(5)



                last_5_away_team_fixtures = df1[(df1['home_team'] == away_team) | (
                    df1['away_team'] == away_team)].sort_values(by='date', ascending=False).head(5)




                # Get common opponents
                #common_opponents = set(last_5_home_team['away_team']).intersection(set(last_5_away_team['away_team']))

                # Get all opponents for Cardiff, including both home and away teams
                last_5_home_team_opps = pd.unique(
                    last_5_home_team_fixtures[["home_team", "away_team"]].stack())

                # Get all opponents for Leicester, including both home and away teams
                last_5_away_team_opps = pd.unique(
                    last_5_away_team_fixtures[["home_team", "away_team"]].stack())

                #
                # Convert NumPy arrays to Python sets
                last_5_home_team_opps_set = set(last_5_home_team_opps)
                last_5_away_team_opps_set = set(last_5_away_team_opps)

                # Find common opponents
                common_opponents = last_5_home_team_opps_set.intersection(
                    last_5_away_team_opps_set)

                # Assuming home_team and away_team are variables that represent the current teams
                current_teams = set([home_team, away_team])
                common_opponents = common_opponents.difference(current_teams)

                # common opponents - home team
                condition_1 = last_5_home_team_fixtures['home_team'].isin(
                    common_opponents) | last_5_home_team_fixtures['away_team'].isin(common_opponents)
                g = last_5_home_team_fixtures[condition_1]

                # common opponents - away team
                condition_2 = last_5_away_team_fixtures['home_team'].isin(
                    common_opponents) | last_5_away_team_fixtures['away_team'].isin(common_opponents)
                h = last_5_away_team_fixtures[condition_2]

                # Check if count of common opponents fixtures is greater than 2
                if len(common_opponents) > 2:
                    if team_won_3_games_or_more(home_team, g) or team_won_3_games_or_more(away_team, h):

                        # Append data to lists for both conditions
                        dates_both_conditions.append(match['date'])
                        home_teams_both_conditions.append(home_team)
                        home_scores_both_conditions.append(match['home_team_score'])
                        away_scores_both_conditions.append(match['away_team_score'])
                        away_teams_both_conditions.append(away_team)
                    # else:
                    #     # Append data to lists for the first condition only
                    #     dates_first_condition.append(match['date'])
                    #     home_teams_first_condition.append(home_team)
                    #     home_scores_first_condition.append(match['home_team_score'])
                    #     away_scores_first_condition.append(match['away_team_score'])
                    #     away_teams_first_condition.append(away_team)

            # Create DataFrames for both conditions
            data_both_conditions = {'Date': dates_both_conditions, 'HomeTeam': home_teams_both_conditions,
                                    'HomeScore': home_scores_both_conditions, 'AwayScore': away_scores_both_conditions, 'AwayTeam': away_teams_both_conditions}
            result_df_both_conditions = pd.DataFrame(data_both_conditions)

            # data_first_condition = {'Date': dates_first_condition, 'HomeTeam': home_teams_first_condition,
            #                         'HomeScore': home_scores_first_condition, 'AwayScore': away_scores_first_condition, 'AwayTeam': away_teams_first_condition}
            # result_df_first_condition = pd.DataFrame(data_first_condition)

            # Create a top-level folder called "results" if it doesn't exist

            # Define file paths for both conditions
            file_path_both_conditions = os.path.join(
                gv.RESULTS_DATA_PATH, "result_for_all_days_both_condition.csv")
            # file_path_first_condition = os.path.join(
            #     results_folder, "result_for_all_days_first_condition.csv")

            # Save both DataFrames to CSV files
            result_df_both_conditions.to_csv(file_path_both_conditions, index=False)
            # result_df_first_condition.to_csv(file_path_first_condition, index=False)

            # Print success messages
            gv.task_log.info(
                f"DataFrame satisfying both conditions saved to: {file_path_both_conditions}")
            # print(
            #    f"DataFrame satisfying the first condition only saved to: {file_path_first_condition}")

            result_df_both_conditions['HomeScore'] = pd.to_numeric(
                result_df_both_conditions['HomeScore'], errors='coerce').astype('Int64')
            result_df_both_conditions['AwayScore'] = pd.to_numeric(
                result_df_both_conditions['AwayScore'], errors='coerce').astype('Int64')

            # result_df_first_condition['HomeScore'] = pd.to_numeric(
            #     result_df_first_condition['HomeScore'], errors='coerce').astype('Int64')
            # result_df_first_condition['AwayScore'] = pd.to_numeric(
            #     result_df_first_condition['AwayScore'], errors='coerce').astype('Int64')

            def filter_fixtures_today_and_2_days_onwards(result_df_both_conditions):
                    # Get the current date in UTC format
                    current_date_utc = pd.to_datetime(datetime.utcnow())

                    # Extract the date part from the 'Date' column in UTC format
                    result_df_both_conditions['Date'] = pd.to_datetime(
                        result_df_both_conditions['Date']).dt.date
                    # result_df_first_condition['Date'] = pd.to_datetime(
                    #     result_df_first_condition['Date']).dt.date


                    # Define the date range for today and the next 2 days
                    date_range = pd.date_range(current_date_utc, periods=3).date

                    # Filter DataFrames based on the date range
                    filtered_df_both_conditions = result_df_both_conditions[
                        result_df_both_conditions['Date'].isin(date_range)]

                    # filtered_df_first_condition = result_df_first_condition[
                    #     result_df_first_condition['Date'].isin(date_range)]

                    return filtered_df_both_conditions

            filtered_both_conditions = filter_fixtures_today_and_2_days_onwards(
                result_df_both_conditions)

            # Print or use the filtered DataFrames as needed
            gv.task_log.info("\nFiltered DataFrame satisfying both conditions for today's, tomorrow's and next tomorrow's fixtures:")
            gv.task_log.info(filtered_both_conditions)

            # print("\nFiltered DataFrame satisfying the first condition only for today's fixtures:")
            # print(filtered_first_condition)


        except Exception as e:
            gv.task_log.warning(f"Error during processing: {e}")

    return filtered_both_conditions
# apply_filtering_logic()


# 



def won_last_5_matches(df):

    def filter_fixtures_today(df):
                # Get the current date in UTC format
                current_date_utc = pd.to_datetime(datetime.utcnow())

                # Extract the date part from the 'Date' column in UTC format
                df['date'] = pd.to_datetime(
                    df['date']).dt.date
                df['date'] = pd.to_datetime(
                    df['date']).dt.date

                # 
                df.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_all_days_across_71_leagues.csv"))
                gv.task_log.info("Saved all fixtures successfully \n")
                # Filter DataFrames based on the current date
                filtered_df_today = df[df['date'] == current_date_utc.date(
                )]
                filtered_df_today = filtered_df_today.drop_duplicates()
                
                filtered_df_today.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_today_across_71_leagues.csv"))
                gv.task_log.info("Save successful. Showing today's fixtures. \n")

                gv.task_log.info(filtered_df_today.drop(columns=["season", "country", "date"], errors='ignore'))
                #return filtered_df_today

#     # Initialize an empty list to store DataFrames
#     dfs = []

# # Initialize final_df to None
#     df = None


#         # Concatenate all DataFrames into one
#         # df = pd.concat(dfs, ignore_index=True)
#     df = pd.read_csv(gv.FIXTURES_DATA_PATH, "all_fixtures_combined.csv")
    df = df.drop_duplicates()
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df['home_team_score'] = pd.to_numeric(
        df['home_team_score'], errors='coerce').astype('Int64')
    df['away_team_score'] = pd.to_numeric(
        df['away_team_score'], errors='coerce').astype('Int64')
    # df['date'] = pd.to_datetime(df['date'], utc=True)
    # Display the first few rows of the resulting DataFrame
    gv.task_log.info("Import successful for all files.\n")
    gv.task_log.info(df.info())
    gv.task_log.info("\n")
    filter_fixtures_today(df)
    gv.task_log.info("\n")


    # Now final_df is accessible outside the try block
    if df is not None:
        try:
            # Do something with final_df
            gv.task_log.info("Applying filtering logic...  This would take some time.\n")
            df1 = df[df["match_status"] == "Match Finished"]

            # Initialize lists to store data
            dates_both_conditions = []
            home_teams_both_conditions = []
            home_scores_both_conditions = []
            away_scores_both_conditions = []
            away_teams_both_conditions = []

            # dates_first_condition = []
            # home_teams_first_condition = []
            # home_scores_first_condition = []
            # away_scores_first_condition = []
            # away_teams_first_condition = []

            def team_won_all_last_5_games(team, g):
                win_count = 0
                for index, row in g.iterrows():
                    if row["home_team"] != team:
                        # Swap home and away team names and scores
                        g.loc[index, "home_team"] = row["away_team"]
                        g.loc[index, "away_team"] = row["home_team"]

                        g.loc[index, "home_team_score"] = row["away_team_score"]
                        g.loc[index, "away_team_score"] = row["home_team_score"]

                # win_count = 0
                # for index, row in g.iterrows():
                #     if g.loc[index, "home_team_score"] > g.loc[index, "away_team_score"]:
                #         win_count += 1
                # if win_count >= 3:
                #     return True
                
                win_count = 0
                for index, row in g.iterrows():
                    if g.loc[index, "home_team_score"] > g.loc[index, "away_team_score"]:
                        win_count += 1
                if win_count == 5:
                    return True

           # Find all match containers
            match_containers = df.to_dict('records')

            # Loop through each match container
            for match in match_containers:
                home_team = match['home_team']
                away_team = match['away_team']

                # Get the last 5 fixtures for both home and away teams
                last_5_home_team_fixtures = df1[(df1['home_team'] == home_team) | (
                    df1['away_team'] == home_team)].sort_values(by='date', ascending=False).head(5)

                last_5_away_team_fixtures = df1[(df1['home_team'] == away_team) | (
                    df1['away_team'] == away_team)].sort_values(by='date', ascending=False).head(5)

                # Assuming home_team and away_team are variables that represent the current teams
                current_teams = set([home_team, away_team])

                # Check if count of common opponents fixtures is greater than 2
                if team_won_all_last_5_games(home_team, last_5_home_team_fixtures) or team_won_all_last_5_games(away_team, last_5_away_team_fixtures):
                    # print(f"Team {home_team} or {away_team} has won all their last 5 matches against any opponent")


                        # Append data to lists for both conditions
                    dates_both_conditions.append(match['date'])
                    home_teams_both_conditions.append(home_team)
                    home_scores_both_conditions.append(match['home_team_score'])
                    away_scores_both_conditions.append(match['away_team_score'])
                    away_teams_both_conditions.append(away_team)
                    # else:
                    #     # Append data to lists for the first condition only
                    #     dates_first_condition.append(match['date'])
                    #     home_teams_first_condition.append(home_team)
                    #     home_scores_first_condition.append(match['home_team_score'])
                    #     away_scores_first_condition.append(match['away_team_score'])
                    #     away_teams_first_condition.append(away_team)

            # Create DataFrames for both conditions
            dataframe_dict = {'Date': dates_both_conditions, 'HomeTeam': home_teams_both_conditions,
                                    'HomeScore': home_scores_both_conditions, 'AwayScore': away_scores_both_conditions, 'AwayTeam': away_teams_both_conditions}
            result_df_both_conditions = pd.DataFrame(dataframe_dict)

            # data_first_condition = {'Date': dates_first_condition, 'HomeTeam': home_teams_first_condition,
            #                         'HomeScore': home_scores_first_condition, 'AwayScore': away_scores_first_condition, 'AwayTeam': away_teams_first_condition}
            # result_df_first_condition = pd.DataFrame(data_first_condition)

            # Create a top-level folder called "results" if it doesn't exist

            # Define file paths for both conditions
            file_path_both_conditions = os.path.join(
                gv.RESULTS_DATA_PATH, "result_for_all_days_c2.csv")
            # file_path_first_condition = os.path.join(
            #     results_folder, "result_for_all_days_first_condition.csv")

            # Save both DataFrames to CSV files
            result_df_both_conditions.to_csv(file_path_both_conditions, index=False)
            # result_df_first_condition.to_csv(file_path_first_condition, index=False)

            # Print success messages
            gv.task_log.info(
                f"DataFrame satisfying both conditions saved to: {file_path_both_conditions}")
            # print(
            #    f"DataFrame satisfying the first condition only saved to: {file_path_first_condition}")

            result_df_both_conditions['HomeScore'] = pd.to_numeric(
                result_df_both_conditions['HomeScore'], errors='coerce').astype('Int64')
            result_df_both_conditions['AwayScore'] = pd.to_numeric(
                result_df_both_conditions['AwayScore'], errors='coerce').astype('Int64')

            # result_df_first_condition['HomeScore'] = pd.to_numeric(
            #     result_df_first_condition['HomeScore'], errors='coerce').astype('Int64')
            # result_df_first_condition['AwayScore'] = pd.to_numeric(
            #     result_df_first_condition['AwayScore'], errors='coerce').astype('Int64')

            def filter_fixtures_today_and_2_days_onwards(result_df_both_conditions):
                    # Get the current date in UTC format
                    current_date_utc = pd.to_datetime(datetime.utcnow())

                    # Extract the date part from the 'Date' column in UTC format
                    result_df_both_conditions['Date'] = pd.to_datetime(
                        result_df_both_conditions['Date']).dt.date
                    # result_df_first_condition['Date'] = pd.to_datetime(
                    #     result_df_first_condition['Date']).dt.date


                    # Define the date range for today and the next 2 days
                    date_range = pd.date_range(current_date_utc, periods=3).date

                    # Filter DataFrames based on the date range
                    filtered_df_both_conditions = result_df_both_conditions[
                        result_df_both_conditions['Date'].isin(date_range)]

                    # filtered_df_first_condition = result_df_first_condition[
                    #     result_df_first_condition['Date'].isin(date_range)]

                    return filtered_df_both_conditions

            filtered_both_conditions = filter_fixtures_today_and_2_days_onwards(
                result_df_both_conditions)

            # Print or use the filtered DataFrames as needed
            gv.task_log.info("\nFiltered DataFrame satisfying both conditions for today's, tomorrow's and next tomorrow's fixtures:")
            gv.task_log.info(filtered_both_conditions)

            # print("\nFiltered DataFrame satisfying the first condition only for today's fixtures:")
            # print(filtered_first_condition)


        except Exception as e:
            gv.task_log.warning(f"Error during processing: {e}")

    return filtered_both_conditions
# apply_filtering_logic()