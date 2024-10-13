from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from twitch_game_analytics import get_top_n_games, get_top_n_games_and_stats, SnowflakeExecutor
from datetime import datetime
import os

N_GAMES = 20

def get_sql(folder, file_name):
    """Helper function to read in contents from a sql script and returns it as a string"""
    
    file_directory = os.path.dirname(__file__)
    sql_file_path = os.path.join(file_directory, f"../sql/{folder}/{file_name}")

    with open(sql_file_path, "r") as file:
        sql_query = file.read()

    return sql_query

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 1),
    "email_on_failure": True,
    "email": ["samtavalalidev@gmail.com"]
}

with DAG(
    "twitch_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
    ) as dag:

    create_temp_games_dim = SnowflakeOperator(
        task_id="create_temp_games_dim",
        snowflake_conn_id="snowflake_conn_id",
        sql=get_sql("games_dim", "create_temp_games_dim.sql")
    )

    @task
    def run_top_n_games(N_GAMES):

        top_n_games_df = get_top_n_games(N_GAMES)

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id")

        with hook.get_conn() as conn:
            sf = SnowflakeExecutor(conn)
            sf.write_pandas_df(top_n_games_df, "temp_games_dim")
        
        print(conn.is_closed())
      

    create_temp_games_dim >> run_top_n_games(N_GAMES)




