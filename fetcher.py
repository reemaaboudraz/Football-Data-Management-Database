import json
import os
import time

from dotenv import load_dotenv
from datetime import datetime
import requests
import psycopg2

load_dotenv()
x_auth_token = os.getenv('X_AUTH_TOKEN')
x_apisports_key = os.getenv('X_APISPORTS_KEY')
football_data_org_headers = {
    "X-Auth-Token": x_auth_token
}
api_football_headers = {
    "x-apisports-key": x_apisports_key,
}
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_params = {
    'dbname': db_name,
    'user': db_user,
    'host': db_host,
    'port': db_port,
}


# Do not try to run commented out methods. They aren't working.

# Fetches leagues and populates the league table
# Param 1: year, this is essential for optimizing fetch in API_Football
def fetch_and_store_leagues(year):
    url = f"https://v3.football.api-sports.io/leagues?season={year}"
    response = requests.get(url, headers=api_football_headers)
    leagues = response.json().get("response")

    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    for league in leagues:
        print(json.dumps(league, indent=4))
        api_football_id = league["league"]["id"]
        league_name = league["league"]["name"]
        league_country = league["country"]["name"]

        cur.execute("""
        INSERT INTO league (name, country, api_football_id)
        VALUES (%s, %s, %s)
        """, (
            league_name, league_country, api_football_id
        ))
    conn.commit()
    print("Successfully fetched leagues.")
    cur.close()
    conn.close()


# Football data org (2nd API)
def fetch_and_store_leagues_api2():
    url = "https://api.football-data.org/v4/competitions/"
    response = requests.get(url, headers=football_data_org_headers)
    leagues = response.json().get("competitions")

    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    print(json.dumps(leagues, indent=4))

    for league in leagues:
        league_name = league["name"]
        league_country = league["area"]["name"]
        cur.execute("""
        SELECT id FROM league WHERE name = %s AND country = %s
        """, (
            league_name, league_country
        ))
        result = cur.fetchall()

        if len(result) == 1:
            fb_org_id = league["id"]
            fb_org_code = league["code"]
            db_id = result[0][0]
            cur.execute("""
            UPDATE league SET fb_org_id = %s, fb_org_league_code = %s WHERE id = %s
            """, (fb_org_id, fb_org_code, db_id))

    conn.commit()
    cur.close()
    conn.close()


def fetch_and_store_teams_api2():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("""
    SELECT * FROM league WHERE fb_org_id IS NOT NULL
    """)
    result = cur.fetchall()

    for row in result:
        db_league_id = row[0]
        league_code = row[5]
        url = f"https://api.football-data.org/v4/competitions/{league_code}/standings"
        response = requests.get(url, headers=football_data_org_headers)
        raw_res = response.json()
        teams = response.json().get("standings")[0]["table"]
        area = raw_res["area"]
        league = raw_res["competition"]

        ext_league_name = league["code"]
        ext_country = area["name"]

        print(json.dumps(area, indent=4))
        print(json.dumps(league, indent=4))
        print(json.dumps(teams, indent=4))
        print("League ID: " + str(db_league_id))
        for team in teams:
            team_id = team["team"]["id"]
            print(team["team"]["name"])
            print(ext_league_name)
            print(ext_country)
            cur.execute("""
            SELECT t.* FROM team t JOIN league l ON t.league_id = %s WHERE t.name = %s AND l.name = %s AND l.country = %s
            """, (db_league_id, team["team"]["name"], ext_league_name, ext_country))

            db_res = cur.fetchall()
            if len(db_res) == 1:
                print(team_id)
                cur.execute("""
                UPDATE team SET fb_org_id = %s WHERE id = %s
                """, (team_id, team))

    conn.commit()
    cur.close()
    conn.close()


# Fetches teams within a league for a specific season.
# Param 1: year - season of the league
# Param 2: api_football_id - to communicate with URL
# Param 3: internal_league_id - to store in team table
# Param 4: cur - cursor for executing queries
def fetch_and_store_teams(year, api_football_league_id, internal_league_id, cur):
    url = f"https://v3.football.api-sports.io/teams?league={api_football_league_id}&season={year}"
    response = requests.get(url, headers=api_football_headers)
    teams = response.json().get("response")

    for team in teams:
        name = team["team"]["name"]
        code = team["team"]["code"]
        founded = team["team"]["founded"]
        national = team["team"]["national"]
        api_football_id = team["team"]["id"]

        cur.execute("""
        INSERT INTO team (name, code, league_id, season, founded, national_team, api_football_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            name, code, internal_league_id, year, founded, national, api_football_id
        ))


# Gets all leagues and stores all teams from that league for a specific season
# Communicates with 'fetch_and_store_teams'
def store_teams():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("SELECT id, api_football_id FROM league")
    rows = cur.fetchall()
    print("Number of leagues: " + str(len(rows)))
    for row in rows:
        fetch_and_store_teams(2022, row[1], row[0], cur)
        print(f"Stored teams from league {row[1]}.")

    conn.commit()
    print("Successfully stored teams.")
    cur.close()
    conn.close()


# Fetch Players
# Param 1: api_football_team_id - ID of the team in the API
# Param 2: internal_team_id - ID of the team in the database
# Param 3: season - the season for fetching player data
# Param 4: cur - cursor for executing queries
def fetch_and_store_players(api_football_team_id, internal_team_id, season, cur):
    url = f"https://v3.football.api-sports.io/players?team={api_football_team_id}&season={season}"
    response = requests.get(url, headers=api_football_headers)
    players = response.json().get("response")

    for player_info in players:
        player = player_info["player"]
        player_id = player["id"]
        first_name = player["firstname"]
        last_name = player["lastname"]
        nationality = player["nationality"]
        position = player_info["statistics"][0]["games"]["position"]

        print("Storing Player: " + first_name + " " + last_name)

        # Insert into the player table
        cur.execute("""
        INSERT INTO player (first_name, last_name, position, nationality, api_football_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """, (
            first_name, last_name, position, nationality, player_id
        ))

        internal_player_id = cur.fetchone()[0]

        # Insert into the team_member table
        cur.execute("""
        INSERT INTO team_member (team_id, player_id, season)
        VALUES (%s, %s, %s)
        """, (
            internal_team_id, internal_player_id, season
        ))


# Fetches player stats and populates the player_stats table
# Param 1: api_football_team_id - ID of the team in the API
# Param 2: season - the season for fetching stats
# Param 3: cur - cursor for executing queries
def fetch_and_store_player_stats(api_football_team_id, season, cur):
    cur.execute("""
    SELECT league_id, id FROM team WHERE api_football_id = %s 
    """, (api_football_team_id,))

    league_id, team_id = cur.fetchone()

    url = f"https://v3.football.api-sports.io/players?team={api_football_team_id}&season={season}"
    response = requests.get(url, headers=api_football_headers)
    players = response.json().get("response")

    for player_info in players:
        player_id = player_info["player"]["id"]
        stats = player_info["statistics"][0]  # Assuming the first entry is the main stats

        appearances = stats["games"]["appearences"]
        goals = stats["goals"]["total"]
        assists = stats["goals"]["assists"]
        yellow_cards = stats["cards"]["yellow"]
        red_cards = stats["cards"]["red"]
        print("Appearances:" + str(appearances))
        # Insert into the player_stats table
        cur.execute("""
        INSERT INTO player_stats (player_id, team_id, league_id, season, games_played, goals, assists, yellow_cards, red_cards)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            player_id, team_id, league_id, season, appearances, goals, assists, yellow_cards, red_cards
        ))


# Fetches all players and stats for teams in the database
# Communicates with 'fetch_and_store_players' and 'fetch_and_store_player_stats'
def store_players_and_stats():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("""
    SELECT t.id, t.api_football_id
    FROM team t
    LEFT JOIN team_member tm ON t.id = tm.team_id
    WHERE t.fb_org_id IS NOT NULL
      AND tm.team_id IS NULL
    LIMIT 10
    """)
    rows = cur.fetchall()
    print("Number of teams: " + str(len(rows)))
    for row in rows:
        print("Row: " + str(row))
        team_id, api_team_id = row
        season = 2022  # Set the season to fetch data
        fetch_and_store_players(api_team_id, team_id, season, cur)
        fetch_and_store_player_stats(api_team_id, season, cur)
        print(f"Stored players and stats for team {api_team_id}.")

        time.sleep(2)

    conn.commit()
    print("Successfully stored players and player stats.")
    cur.close()
    conn.close()


# def store_player_salaries():
#     conn = psycopg2.connect(**db_params)
#     print("DB connection successful.")
#     cur = conn.cursor()


# Fetches transfers for players in a specific team during a season
# Param 1: api_football_team_id - ID of the team in the API
# Param 2: internal_team_id - ID of the team in the database
# Param 3: season - the season for fetching transfer data
# Param 4: cur - cursor for executing queries
# def fetch_and_store_transfers(api_football_team_id, internal_team_id, season, cur):
#     url = f"https://v3.football.api-sports.io/transfers?team={api_football_team_id}&season={season}"
#     response = requests.get(url, headers=api_football_headers)
#     transfers = response.json().get("response")
#
#     if transfers is None or len(transfers) == 0:
#         print(f"No transfer data available for team {api_football_team_id} during season {season}")
#         return
#
#     for transfer_info in transfers:
#         player_id = transfer_info["player"]["id"]
#         player_name = transfer_info["player"]["name"]
#         from_team = transfer_info["team"]["from"]["name"] if "from" in transfer_info["team"] else None
#         to_team = transfer_info["team"]["to"]["name"] if "to" in transfer_info["team"] else None
#         transfer_date = transfer_info["date"]
#         transfer_fee = transfer_info["fee"] if "fee" in transfer_info else None
#
#         # Insert transfer data into the transfers table
#         cur.execute("""
#         INSERT INTO transfers (player_id, from_team, to_team, transfer_date, transfer_fee, season)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         """, (
#             player_id, from_team, to_team, transfer_date, transfer_fee, season
#         ))


# Fetches all transfer data for teams in the database
# Communicates with 'fetch_and_store_transfers'
# def store_transfers():
#     conn = psycopg2.connect(**db_params)
#     print("DB connection successful.")
#     cur = conn.cursor()
#
#     cur.execute("SELECT id, api_football_id FROM team")
#     rows = cur.fetchall()
#     for row in rows:
#         team_id, api_team_id = row
#         season = 2022  # Set the season to fetch data
#         fetch_and_store_transfers(api_team_id, team_id, season, cur)
#         print(f"Stored transfers for team {api_team_id}.")
#
#     conn.commit()
#     print("Successfully stored transfers.")
#     cur.close()
#     conn.close()


# def fetch_and_store_teamstats(season, cur):
#     cur.execute("""
#     SELECT id, api_football_id FROM league LIMIT 20
#     """)
#     leagues = cur.fetchall()
#     print(leagues)
#     for league in leagues:
#         league = league[1]
#         url = f"https://v3.football.api-sports.io/standings?league={league}&season={season}"
#         response = requests.get(url, headers=api_football_headers)
#         global_response = response.json().get("response")
#         print(json.dumps(global_response, indent=4))
#         standings = global_response[0].get("standings")
#         for standing in standings:
#             rank = standing["rank"]
#             api_football_team_id = standing["team"]["id"]
#             cur.execute("""
#             SELECT id FROM team WHERE api_football_id = %s
#             """, (api_football_team_id,))
#             internal_team_id = cur.fetchone()[0]
#             games_played = standing["all"]["played"]
#             wins = standing["all"]["win"]
#             losses = standing["all"]["lose"]
#             draws = standing["all"]["draw"]
#             points = standing["points"]
#             goals_scored = standing["all"]["goals"]["for"]
#             goals_against = standing["all"]["goals"]["against"]
#             goal_difference = standing["goalsDiff"]
#             cur.execute("""
#             INSERT INTO team_stats (team_id, league_id, rank, games_played, wins, losses, draws, points, goals_for, goals_against, goal_difference)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """, (
#                 internal_team_id, league, rank, games_played, wins, losses, draws, points, goals_scored, goals_against,
#                 goal_difference
#             ))
#         print(f"Stored standings for league {league}.")

# def store_teamstats():
#     conn = psycopg2.connect(**db_params)
#     print("DB connection successful.")
#     cur = conn.cursor()
#     fetch_and_store_teamstats(2022, cur)
#     conn.commit()
#     cur.close()
#     conn.close()

# store_teams()
# store_players_and_stats()

def store_teams_for_leagues_in_api2():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("""
    SELECT * FROM league WHERE fb_org_id IS NOT NULL
    """)
    leagues = cur.fetchall()
    for league in leagues:
        print(league)
        fetch_and_store_teams(2022, league[3], league[0], cur)

    conn.commit()
    cur.close()
    conn.close()


def set_fb_org_id_in_team_db():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("""
    SELECT * FROM league WHERE fb_org_id IS NOT NULL
    """)
    leagues = cur.fetchall()
    for league in leagues:
        league_code = league[5]
        db_league_id = league[0]
        cur.execute("""
        SELECT * FROM team WHERE league_id = %s
        """, (league[0],))
        teams = cur.fetchall()

        url = f"https://api.football-data.org/v4/competitions/{league_code}/standings"

        response = requests.get(url, headers=football_data_org_headers)
        raw_res = response.json()
        teams = response.json().get("standings")[0]["table"]
        print("TEAMS:\n", teams)
        area = raw_res["area"]
        league = raw_res["competition"]
        ext_league_name = league["name"]
        ext_country = area["name"]
        print(ext_league_name, ext_country)
        for team in teams:
            print("Query Parameters:")
            print("Team Short Name (ILIKE):", f"%{team['team']['shortName']}%")
            print("League Name:", ext_league_name)
            print("Country:", ext_country)

            team_name = team["team"]["name"].replace("-", " ").replace(" FC", "").replace(" SCO", "")
            team_short_name = team["team"]["shortName"].replace("RC ", "").replace("Stade de ", "")

            cur.execute("""
            SELECT t.* FROM team t JOIN league l ON t.league_id = l.id WHERE (t.name ILIKE %s OR t.name ILIKE %s) AND l.name = %s AND l.country = %s
            """, (f"%{team_name}%", f"%{team_short_name}%", ext_league_name, ext_country))
            records = cur.fetchall()
            if len(records) > 0:
                team_id = records[0][7]
                print("Records[0]: ", str(team_id))
                print("TEAM MATCHES FOUND: " + str(len(records)))
                print("UPDATING TEAM: " + str(team_id) + " with fb_org_id " + str(team["team"]["id"]))
                cur.execute("""
                UPDATE team SET fb_org_id = %s WHERE api_football_id = %s
                """, (team["team"]["id"], team_id))

    conn.commit()
    cur.close()
    conn.close()


def store_coach_for_team():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("""
    SELECT t.id, t.fb_org_id FROM team t 
    WHERE fb_org_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM coach c
        WHERE c.team_id = t.id
      );
    """)
    teams = cur.fetchall()

    for team in teams:
        team_id = team[0]
        fb_org_team_id = team[1]
        url = f"https://api.football-data.org/v4/teams/{fb_org_team_id}"
        response = requests.get(url, headers=football_data_org_headers)
        raw_res = response.json()
        try:
            coach = raw_res["coach"]
        except KeyError:
            print(f"Sleeping for 67 seconds to account for HTTP 229...")
            time.sleep(67)
            try:
                response = requests.get(url, headers=football_data_org_headers)
                raw_res = response.json()
                coach = raw_res["coach"]
                conn.commit()
            except KeyError:
                print(f"Coach not found for team {fb_org_team_id}")
                break

        coach_fb_org_id = coach["id"]
        first_name = coach["firstName"]
        last_name = coach["lastName"]
        nationality = coach["nationality"]
        contract = coach["contract"]
        raw_contract_start = contract["start"]
        raw_contract_end = contract["until"]
        print(raw_contract_start, raw_contract_end)
        if raw_contract_start is not None and raw_contract_end is not None:
            contract_start = raw_contract_start + "-01"
            contract_end = raw_contract_end + "-01"

            start_date_obj = datetime.strptime(contract_start, '%Y-%m-%d')
            end_date_obj = datetime.strptime(contract_end, '%Y-%m-%d')
        else:
            start_date_obj = None
            end_date_obj = None

        print(f"Getting coach for team {str(fb_org_team_id)}.\nHead Coach: {first_name} {last_name}\n")

        cur.execute("""
        INSERT INTO coach (fb_org_id, first_name, last_name, team_id, nationality, contract_start, contract_end)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (coach_fb_org_id, first_name, last_name, team_id, nationality, start_date_obj, end_date_obj))

    conn.commit()
    cur.close()
    conn.close()


def store_club_colors_for_team():
    conn = psycopg2.connect(**db_params)
    print("DB connection successful.")
    cur = conn.cursor()

    cur.execute("""
    SELECT id, fb_org_id FROM team WHERE fb_org_id IS NOT NULL AND club_colors IS NULL
    """)

    teams = cur.fetchall()
    for team in teams:
        team_id = team[0]
        team_fb_org_id = team[1]
        url = f"https://api.football-data.org/v4/teams/{team_fb_org_id}"
        response = requests.get(url, headers=football_data_org_headers)
        raw_res = response.json()

        try:
            club_colors = raw_res["clubColors"]
        except KeyError:
            print(f"Sleeping for 67 seconds to account for HTTP 229...")
            time.sleep(67)
            try:
                response = requests.get(url, headers=football_data_org_headers)
                raw_res = response.json()
                club_colors = raw_res["clubColors"]
                conn.commit()
            except KeyError:
                print(f"Coach not found for team {team_fb_org_id}")
                break

        print("Getting club colors for team " + str(team_fb_org_id) + ".\n")
        print(club_colors, team_id)
        cur.execute("""
        UPDATE team SET club_colors = %s WHERE id = %s
        """, (club_colors, team_id))

    conn.commit()
    cur.close()
    conn.close()


# store_club_colors_for_team()
# store_coach_for_team()
# set_fb_org_id_in_team_db()
# store_teams()
# store_teams_for_leagues_in_api2()
# store_players_and_stats()
# fetch_and_store_teams_api2()
# fetch_and_store_leagues(2022)
# fetch_and_store_leagues_api2()
