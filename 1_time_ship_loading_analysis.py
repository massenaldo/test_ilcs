from psycopg2.extras import execute_batch
from io import StringIO
from fpdf import FPDF
import pandas as pd
import os
import psycopg2


# Declare global variable
FILE_PATH = '/Users/aldomasendi/Documents/PELINDO/source/'
FILE_RESULT_PATH = '/Users/aldomasendi/Documents/PELINDO/result/'
FILE_NAME = 'port_operations.csv'

# Access to DB
config_pg = {
        'database'  : 'postgres',
        'user'      : 'postgres',
        'password'  : 'pass',
        'host'      : '127.0.0.1',
        'port'      : '5432'
}

# Function and Procedure
def ingest_to_pg(df,curr_pg,conn_pg,table):

    # Initialize a string buffer
    sio = StringIO()
    sio.write(df.to_csv(index=False,header=True, sep=','))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream
    
    # Copy the string buffer to the database, as if it were an actual file
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    curr_pg.copy_expert(sql=sql % table, file=sio)
    conn_pg.commit()

# Read file from CSV
df = pd.read_csv(os.path.join(FILE_PATH,FILE_NAME),sep=',')

# Calculate Average Operation Time Every Ship
avg_time_ship = df[['ship_id','operation_time']].groupby('ship_id').mean().reset_index()

# Eviciency Crane from Operation Time
eviciency_crane = df[['crane_id','operation_time']].groupby('crane_id').sum().sort_values('operation_time').head(1).reset_index()

## Insert Result to Database ##
# Create table for the results
query_create_table_avg_time_ship = '''
    CREATE TABLE IF NOT EXISTS public.report_time_ship (
        ship_id varchar null,
        avg_operation_time float8 null
    );
    CREATE INDEX IF NOT EXISTS ship_id_idx ON public.report_time_ship using btree(ship_id);
'''

query_create_table_eviciency_crane = '''
    CREATE TABLE IF NOT EXISTS public.report_eviciency_crane (
        crane_id varchar null,
        total_operation_time float8 null
    );
    CREATE INDEX IF NOT EXISTS crane_id_idx ON public.report_eviciency_crane using btree(crane_id);
'''

# Connect to database
try:
    print("try connect to database")
    conn_pg = psycopg2.connect(database = config_pg['database'],
                            user = config_pg['user'],
                            password = config_pg['password'],
                            host = config_pg['host'],
                            port = config_pg['port'])

    curr_pg = conn_pg.cursor()
    print("Successfully connect to database")

    # create table
    print("create table avg_time_ship")
    curr_pg.execute(query_create_table_avg_time_ship)
    conn_pg.commit()
    print("create table eviciency_crane")
    curr_pg.execute(query_create_table_eviciency_crane)
    conn_pg.commit()

    # truncate table
    print("Truncate table...")
    curr_pg.execute("TRUNCATE TABLE public.report_time_ship")
    conn_pg.commit()
    curr_pg.execute("TRUNCATE TABLE public.report_eviciency_crane")
    conn_pg.commit()

    # insert the result
    print("Inserting data...")
    ingest_to_pg(avg_time_ship,curr_pg,conn_pg,'public.report_time_ship')
    ingest_to_pg(eviciency_crane,curr_pg,conn_pg,'public.report_eviciency_crane')

    print("Successfully insert data...")
    curr_pg.close()
    conn_pg.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(f"Error : {error}")

## Create Report Weekly
# Date to Weekly
df['arrival_time'] = pd.to_datetime(df['arrival_time'] )
df['weekly_start'] = df['arrival_time'].dt.to_period('W').apply(lambda r: r.start_time)
df['weekly_end'] = df['weekly_start'] + pd.Timedelta(days=6)
df['period'] = df['weekly_start'].dt.strftime('%Y-%m-%d') + ' - ' + df['weekly_end'].dt.strftime('%Y-%m-%d')

# 1. Check the longest operation time ship
longest_time_ship = df.loc[df.groupby(['period'])['operation_time'].idxmax()]

# 2. Calculate Top and Worst Crane
total_weight_and_time_crane = df[['period','crane_id','cargo_weight','operation_time']].groupby(['period','crane_id']).agg({'cargo_weight':'sum','operation_time':'sum'}).reset_index()
total_weight_and_time_crane['weight_per_hour'] = total_weight_and_time_crane['cargo_weight']/total_weight_and_time_crane['operation_time']
rank_crane = total_weight_and_time_crane.sort_values(['period','weight_per_hour'],ascending=False)

# 3. Create Report Weekly
report_text = f"""
Weekly Report Loading and Unloading Time Operation\n
\n
The port has already finished loading and unloading the cargo in this month. Below is the weekly report details:\n
\n
Week 1 (2024-12-02 - 2024-12-08)\n
    -   The ship has a longest loading and unloading time is {longest_time_ship.loc[longest_time_ship['period'] == '2024-12-02 - 2024-12-08','ship_id'].values[0]}\n
        and The ship is taken time by {int(longest_time_ship.loc[longest_time_ship['period'] == '2024-12-02 - 2024-12-08','operation_time'].values[0])} hours\n
    -   The Crane with Top Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-02 - 2024-12-08','crane_id'].head(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-02 - 2024-12-08','cargo_weight'].head(1).values[0])} kg\n
        and The Crane with Worst Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-02 - 2024-12-08','crane_id'].tail(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-02 - 2024-12-08','cargo_weight'].tail(1).values[0])} kg\n
\n
Week 2 (2024-12-09 - 2024-12-15)\n
    -   The ship has a longest loading and unloading time is {longest_time_ship.loc[longest_time_ship['period'] == '2024-12-09 - 2024-12-15','ship_id'].values[0]}\n
        and The ship is taken time by {int(longest_time_ship.loc[longest_time_ship['period'] == '2024-12-09 - 2024-12-15','operation_time'].values[0])} hours\n
    -   The Crane with Top Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-09 - 2024-12-15','crane_id'].head(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-09 - 2024-12-15','cargo_weight'].head(1).values[0])} kg\n
        and The Crane with Worst Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-09 - 2024-12-15','crane_id'].tail(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-09 - 2024-12-15','cargo_weight'].tail(1).values[0])} kg\n
\n    
Week 3 (2024-12-16 - 2024-12-22)\n
    -   The ship has a longest loading and unloading time is {longest_time_ship.loc[longest_time_ship['period'] == '2024-12-16 - 2024-12-22','ship_id'].values[0]}\n
        and The ship is taken time by {int(longest_time_ship.loc[longest_time_ship['period'] == '2024-12-16 - 2024-12-22','operation_time'].values[0])} hours\n
    -   The Crane with Top Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-16 - 2024-12-22','crane_id'].head(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-16 - 2024-12-22','cargo_weight'].head(1).values[0])} kg\n
        and The Crane with Worst Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-16 - 2024-12-22','crane_id'].tail(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-16 - 2024-12-22','cargo_weight'].tail(1).values[0])} kg\n
\n
Week 4 (2024-12-23 - 2024-12-29)\n
    -   The ship has a longest loading and unloading time is {longest_time_ship.loc[longest_time_ship['period'] == '2024-12-23 - 2024-12-29','ship_id'].values[0]}\n
        and The ship is taken time by {int(longest_time_ship.loc[longest_time_ship['period'] == '2024-12-23 - 2024-12-29','operation_time'].values[0])} hours\n
    -   The Crane with Top Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-23 - 2024-12-29','crane_id'].head(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-23 - 2024-12-29','cargo_weight'].head(1).values[0])} kg\n
        and The Crane with Worst Performance is {rank_crane.loc[rank_crane['period'] == '2024-12-23 - 2024-12-29','crane_id'].tail(1).values[0]} with average cargo weight loading per hour in {int(rank_crane.loc[rank_crane['period'] == '2024-12-23 - 2024-12-29','cargo_weight'].tail(1).values[0])} kg\n
\n
The following is a weekly report of the ship unloading operation time.\n
"""

# 4. save to PDF
pdf=FPDF()
pdf.add_page()
pdf.set_font('Times','',12)
lines = report_text.split('\n')
for line in lines:
    pdf.cell(0, 3, txt=line, ln=True)
pdf.output(os.path.join(FILE_RESULT_PATH,'time_ship_loading_analysis.pdf'),'F')