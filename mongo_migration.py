import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from bson import SON, ObjectId

load_dotenv()
# PostgreSQL connection parameters
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
pg_params = {
    'dbname': db_name,
    'user': db_user,
    'host': db_host,
    'port': db_port,
}

# MongoDB connection parameters
mongo_uri = 'mongodb+srv://everyone:everyone@macmeecluster.he3iw.mongodb.net/'


def migrate_data():
    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()

    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client["footballdb"]

    players_collection = mongo_db["players"]
    teams_collection = mongo_db["teams"]
    team_members_collection = mongo_db["team_members"]
    leagues_collection = mongo_db["leagues"]
    player_stats_collection = mongo_db["player_stats"]

    pg_cursor.execute("SELECT * FROM player")
    players = pg_cursor.fetchall()

    pg_cursor.execute("SELECT * FROM team")
    teams = pg_cursor.fetchall()

    pg_cursor.execute("SELECT * FROM team_member")
    team_members = pg_cursor.fetchall()

    pg_cursor.execute("SELECT * FROM league")
    leagues = pg_cursor.fetchall()

    pg_cursor.execute("SELECT * FROM player_stats")
    player_stats = pg_cursor.fetchall()

    league_id_map = {}
    for league in leagues:
        league_data = {
            "name": league[1],
            "country": league[2],
            "apiFootballId": league[3],
            "fbOrgId": league[4],
            "fbOrgLeagueCode": league[5]
        }
        result = leagues_collection.insert_one(league_data)
        league_id_map[league[0]] = result.inserted_id

    player_id_map = {}
    for player in players:
        player_data = {
            "playerId": player[0],
            "firstName": player[1],
            "lastName": player[2],
            "position": player[3],
            "nationality": player[4]
        }
        result = players_collection.insert_one(player_data)
        player_id_map[player[0]] = result.inserted_id

    team_id_map = {}
    for team in teams:
        team_data = {
            "teamId": team[0],
            "name": team[1],
            "code": team[2],
            "season": team[3],
            "founded": team[4],
            "nationalTeam": team[5],
            "apiFootballId": team[6],
            "fbOrgId": team[7],
            "clubColors": team[8],
            # Link to the league in MongoDB by storing the ObjectId
            "leagueId": league_id_map.get(team[2])
        }
        result = teams_collection.insert_one(team_data)
        team_id_map[team[0]] = result.inserted_id

    for tm in team_members:
        player_object_id = player_id_map.get(tm[0])
        team_object_id = team_id_map.get(tm[1])

        if player_object_id and team_object_id:
            team_member_data = {
                "playerId": player_object_id,
                "teamId": team_object_id,
                "season": tm[2]
            }
            team_members_collection.insert_one(team_member_data)

    for ps in player_stats:
        player_object_id = player_id_map.get(ps[0])
        team_object_id = team_id_map.get(ps[1])
        league_object_id = league_id_map.get(ps[2])

        if player_object_id and team_object_id and league_object_id:
            player_stat_data = {
                "playerId": player_object_id,
                "teamId": team_object_id,
                "leagueId": league_object_id,
                "season": ps[3],
                "gamesPlayed": ps[4],
                "goals": ps[5],
                "assists": ps[6],
                "yellowCards": ps[7],
                "redCards": ps[8],
                "marketValue": ps[9]
            }
            player_stats_collection.insert_one(player_stat_data)

    pg_cursor.close()
    pg_conn.close()

    print("Migration complete!")

def migrate_coaches_data():
    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()

    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client["footballdb"]
    coaches_collection = mongo_db["coaches"]
    teams_collection = mongo_db["teams"]

    team_mapping = {}
    for team in teams_collection.find():
        team_mapping[team['teamId']] = team['_id']

    pg_cursor.execute("""
        SELECT id, first_name, last_name, fb_org_id, team_id, nationality, contract_start, contract_end
        FROM coach
    """)
    coaches = pg_cursor.fetchall()

    for coach in coaches:
        team_object_id = team_mapping.get(coach[4])

        if team_object_id is None:
            print(f"Team ID {coach[4]} not found for coach {coach[1]} {coach[2]}")
            continue

        coach_data = {
            "_id": ObjectId(),
            "first_name": coach[1],
            "last_name": coach[2],
            "fb_org_id": coach[3],
            "team_id": team_object_id,
            "nationality": coach[5],
            "contract_start": coach[6] if coach[6] else None,
            "contract_end": coach[7] if coach[7] else None
        }
        coaches_collection.insert_one(coach_data)

    pg_cursor.close()
    pg_conn.close()

    print("Coaches migration completed.")

migrate_coaches_data()
#migrate_data()
