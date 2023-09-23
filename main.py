import sqlite3
import pandas as pd
from datetime import datetime
import os
import psutil


def create_table(connection):
    cursor = connection.cursor()
    # drop для того чтобы при каждом запуске в таблице были данные только за 1 посчитанный день
    cursor.execute("DROP TABLE banned_players")
    cursor.execute("""CREATE TABLE IF NOT EXISTS banned_players (
                        timestamp INTEGER,
                        player_id INTEGER PRIMARY KEY,
                        event_id INTEGER,
                        error_id INTEGER,
                        json_server TEXT,
                        json_client TEXT)
                        """)
    cursor.close()
    connection.commit()


def load_error_data(date):
    client_df = pd.read_csv('client.csv', sep=',')
    client_df = client_df.assign(date=[datetime.fromtimestamp(x).date() for x in client_df['timestamp']])
    client_df_filtered = client_df.loc[client_df['date'] == date]

    server_df = pd.read_csv('server.csv', sep=',')
    server_df = server_df.assign(date=[datetime.fromtimestamp(x).date() for x in server_df['timestamp']])
    server_df_filtered = server_df.loc[server_df['date'] == date]

    joined_data = client_df_filtered.merge(server_df_filtered, on='error_id', how='inner',
                                           suffixes=('_client', '_server'))
    return joined_data


def load_cheaters(connection):
    cursor = connection.cursor()
    res = cursor.execute('SELECT * FROM cheaters')
    data = res.fetchall()
    cursor.close()
    ban_df = pd.DataFrame(data, columns=['player_id', 'ban_time'])
    ban_df['ban_time'] = pd.to_datetime(ban_df['ban_time'])
    return ban_df


def import_to_database(connection, ban_players):
    ban_players = ban_players[['timestamp_server', 'player_id', 'event_id', 'error_id',
                               'description_server', 'description_client']].values
    sql = ("INSERT INTO banned_players (timestamp, player_id, event_id, error_id, json_server, json_client) "
           "VALUES (?, ?, ?, ?, ?, ?)")
    cursor = connection.cursor()
    cursor.executemany(sql, ban_players)
    connection.commit()


def run():
    connection = sqlite3.connect('cheaters.db')
    create_table(connection)
    string_date = input('Введите дату в формате День-Месяц-Год (Мин. Дата 01-01-2021 - Макс. Дата 31-05-2021): ')
    error_data = load_error_data(datetime.strptime(string_date, "%d-%m-%Y").date())
    cheaters_data = load_cheaters(connection)
    all_data = error_data.merge(cheaters_data, left_on='player_id', right_on='player_id',
                                suffixes=('_error', '_cheaters'))
    all_data = all_data.assign(date_diff=[(x - datetime.date(y)).days for x, y in
                                          zip(all_data['date_server'], all_data['ban_time'])])
    ban_players = all_data.loc[all_data['date_diff'] >= 1]
    import_to_database(connection, ban_players)

    process = psutil.Process()
    print(f"Программа занимает в пике {process.memory_info().peak_wset / 1024 / 1024} MB памяти")


if __name__ == '__main__':
    run()
