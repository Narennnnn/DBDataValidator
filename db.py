import psycopg2
import pandas as pd
import plotly.graph_objects as go
import numpy as np


def check_date_continuation(df):
    df['datetime'] = pd.to_datetime(df['timestamp'].astype(np.int64), unit='ms')
    df['Time Difference'] = df['datetime'].diff().dt.total_seconds()
    print(df[['datetime', 'Time Difference']])

    trace = go.Scatter(
        x=df['datetime'],
        y=df['Time Difference']
    )

    layout = go.Layout(
        xaxis=dict(title='Datetime'),
        yaxis=dict(title='Time Difference in seconds')
    )
    fig = go.Figure(data=[trace], layout=layout)
    fig.show()


def connect_db():
    try:
        # Define PostgreSQL connection parameters
        conn_params = {
            'host': 'localhost',
            'port': '5432',
            'user': 'postgres',
            'password': 'pass',
            'database': 'postgres'
        }

        # Create a connection to the database
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        return cursor, connection
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None, None


if __name__ == '__main__':
    try:
        # Connect to the database
        cursor, connection = connect_db()

        if cursor and connection:
            # SQL query to fetch data
            query = """
                SELECT * FROM openinterest
    WHERE symbol='BTCUSDT'
     AND exchange = 'Binance'
ORDER BY timestamp ASC;

            """

            # Execute the SQL query
            cursor.execute(query)

            # Fetch all rows from the result set
            rows = cursor.fetchall()

            if rows:
                # Get column names
                column_names = [desc[0] for desc in cursor.description]

                # Create a DataFrame
                df = pd.DataFrame(rows, columns=column_names)

                # Print the DataFrame
                print(df)

                # Visualize data continuity
                check_date_continuation(df)
            else:
                print("No data found in the result set.")

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
