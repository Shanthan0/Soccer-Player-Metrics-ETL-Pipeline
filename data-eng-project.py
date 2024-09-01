import os
from collections import defaultdict
from sqlite3 import Cursor
from typing import Dict, List

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

# Constants
load_dotenv("data-eng-project.env")

# Constants
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
STAGING_TABLE_NAME = "player_offensive_metrics_staging"
PROD_TABLE_NAME = "player_offensive_metrics_prod"
EVENTS_URL = "https://api.github.com/repos/statsbomb/open-data/contents/data/events/"

# Initialize defaultdict for offensive metrics
offensive_metrics = defaultdict(
    lambda: {
        "name": None,
        "goals": 0,
        "assists": 0,
        "shots": 0,
        "shots_on_target": 0,
        "dribbles_completed": 0,
        "key_passes": 0,
        "crosses": 0,
    }
)


def fetch_event_files(url: str, headers: Dict[str, str]):
    """Fetch the list of event files from the GitHub repository."""
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def process_event_data(file_data):
    """Process the event data to extract offensive metrics."""
    for event in file_data.json():
        try:
            event_type = event["type"]["name"]
            player_id = event.get("player", {}).get("id")
            player_name = event.get("player", {}).get("name")

            if not player_id:
                continue  # Skip if no player is associated with the event

            offensive_metrics[player_id]["name"] = player_name

            if event_type == "Shot":
                offensive_metrics[player_id]["shots"] += 1
                if event.get("shot", {}).get("outcome", {}).get("name") == "Goal":
                    offensive_metrics[player_id]["goals"] += 1
                if event.get("shot", {}).get("outcome", {}).get("name") in {
                    "Goal",
                    "Saved",
                }:
                    offensive_metrics[player_id]["shots_on_target"] += 1

            if event_type == "Pass":
                if event.get("pass", {}).get("goal_assist"):
                    offensive_metrics[player_id]["assists"] += 1
                if event.get("pass", {}).get("shot_assist"):
                    offensive_metrics[player_id]["key_passes"] += 1
                if event.get("pass", {}).get("cross"):
                    offensive_metrics[player_id]["crosses"] += 1

            if event_type == "Dribble":
                if (
                    event.get("dribble", {}).get("outcome", {}).get("name")
                    == "Complete"
                ):
                    offensive_metrics[player_id]["dribbles_completed"] += 1

        except Exception as e:
            print(f"Error processing event: {event}")
            print(f"Error message: {e}")
            continue


def drop_table(cursor: Cursor, table_name: str):
    """Drop the table if it exists."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def create_table(cursor: Cursor, table_name: str, columns: List[str]):
    """Create a table in the PostgreSQL database if it doesn't already exist."""
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for col in columns:
        sql += f'"{col}" VARCHAR(255),'
    sql = sql.rstrip(",") + ")"
    cursor.execute(sql)


def insert_into_prod_table(cursor: Cursor, staging_table: str, prod_table: str):
    """Insert data from the staging table into the production table with type casting."""
    insert_sql = f"""
    INSERT INTO {prod_table}
    SELECT
        player_id,
        name,
        goals::int,
        assists::int,
        shots::int,
        shots_on_target::int,
        dribbles_completed::int,
        key_passes::int,
        crosses::int
    FROM {staging_table}"""
    cursor.execute(insert_sql)


def main():
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {GITHUB_TOKEN}",
    }

    # Fetch event files from GitHub
    dir_data = fetch_event_files(EVENTS_URL, headers)

    for res in dir_data:
        print(f"Processing {res['download_url']}")
        file_data = requests.get(res["download_url"])
        process_event_data(file_data)

    # Convert metrics to DataFrame
    df = pd.DataFrame.from_dict(offensive_metrics, orient="index")
    df.index.name = "player_id"
    df.reset_index(inplace=True)

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
    )

    try:
        with conn.cursor() as cur:
            # Drop and create staging table
            drop_table(cur, STAGING_TABLE_NAME)
            create_table(cur, STAGING_TABLE_NAME, df.columns)

            # Save DataFrame to CSV for COPY command
            csv_path = f"/Users/sreeshanthankuthuru/Downloads/{STAGING_TABLE_NAME}.csv"
            df.to_csv(csv_path, index=False)

            # Try copying data into the staging table
            copy_sql = f"""
            COPY {STAGING_TABLE_NAME} FROM '{csv_path}'
            WITH DELIMITER ',' CSV HEADER;
            """
            try:
                cur.execute(copy_sql)
                print("Data copied to staging table successfully.")

                # Drop and create production table only after successful copy
                drop_table(cur, PROD_TABLE_NAME)
                create_table(cur, PROD_TABLE_NAME, df.columns)
                conn.commit()

            except Exception as e:
                print(f"Error copying data to staging table: {e}")
                conn.rollback()

            # Try inserting data into production table from staging table
            try:
                insert_into_prod_table(cur, STAGING_TABLE_NAME, PROD_TABLE_NAME)
                print("Data inserted into production table successfully.")
                conn.commit()

            except Exception as e:
                print(f"Error inserting into production table: {e}")
                conn.rollback()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
