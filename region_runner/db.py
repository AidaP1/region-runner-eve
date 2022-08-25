
from dataclasses import dataclass
import psycopg2, psycopg2.extras, click, os
import pandas as pd
from flask import current_app, g

def get_db():
    if 'db' not in g:
        # pulls from heroku 
        DATABASE_URL = os.environ['DATABASE_URL']
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        g.db = psycopg2.connect(
            DATABASE_URL, 
            sslmode='require')

    return g.db

'''
Gets the data into the DB
Will delete all the existing data, under the assumption that the new data is the full, latest set
'''
def execute_values(conn, df, table):
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = 'INSERT INTO %s(%s) VALUES %%s' % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM %s' % (table))
        psycopg2.extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_db():
    db = get_db()
    cursor = db.cursor()
    command = '''
        DROP TABLE IF EXISTS structures;
        DROP TABLE IF EXISTS stations;
        DROP TABLE IF EXISTS systems;
        DROP TABLE IF EXISTS regions;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS types;
        DROP TABLE IF EXISTS order_history;

        CREATE TABLE structures (
            structureID BIGINT PRIMARY KEY,
            solarSystemID INTEGER DEFAULT NULL,
            regionID INTEGER DEFAULT NULL,
            structureName TEXT DEFAULT NULL
        );
        
        CREATE TABLE stations (
            stationID INTEGER PRIMARY KEY,
            security REAL DEFAULT NULL,
            dockingCostPerVolume REAL DEFAULT NULL,
            maxShipVolumeDockable REAL DEFAULT NULL,
            officeRentalCost INTEGER DEFAULT NULL,
            operationID INTEGER DEFAULT NULL,
            stationTypeID INTEGER DEFAULT NULL,
            corporationID INTEGER DEFAULT NULL,
            solarSystemID INTEGER DEFAULT NULL,
            constellationID INTEGER DEFAULT NULL,
            regionID INTEGER DEFAULT NULL,
            stationName TEXT DEFAULT NULL,
            reprocessingEfficiency REAL DEFAULT NULL,
            reprocessingStationsTake REAL DEFAULT NULL,
            reprocessingHangarFlag INTEGER DEFAULT NULL
        );

        CREATE TABLE systems (
            index INTEGER,
            regionID INTEGER,
            constellationID INTEGER,
            solarSystemID INTEGER PRIMARY KEY,
            solarSystemName TEXT,
            luminosity REAL,
            border INTEGER,
            fringe INTEGER,
            corridor INTEGER,
            hub INTEGER,
            international INTEGER,
            regional INTEGER,
            constellation TEXT,
            security REAL,
            factionID TEXT,
            radius REAL,
            sunTypeID TEXT,
            securityClass TEXT
        );

        CREATE TABLE regions (
            index INTEGER,
            regionID INTEGER PRIMARY KEY,
            regionName TEXT,
            factionID TEXT,
            nebula INTEGER,
            radius TEXT
        );

        CREATE TABLE orders (
            index INTEGER,
            duration INTEGER,
            is_buy_order INTEGER,
            issued TEXT,
            location_id INTEGER,
            min_volume INTEGER,
            order_id INTEGER PRIMARY KEY,
            price REAL,
            range TEXT,
            system_id INTEGER,
            type_id INTEGER,
            volume_remain INTEGER,
            volume_total INTEGER,
            extracted_timestamp TEXT
        );

        CREATE TABLE types (
            index INTEGER,
            typeID INTEGER PRIMARY KEY,
            groupID INTEGER,
            typeName TEXT,
            description TEXT,
            mass REAL,
            volume REAL,
            capacity REAL,
            portionSize INTEGER,
            raceID TEXT,
            basePrice TEXT,
            published INTEGER,
            marketGroupID TEXT,
            iconID TEXT,
            soundID TEXT,
            graphicID INTEGER
        );

        CREATE TABLE order_history (
            index INTEGER PRIMARY KEY,
            average REAL,
            date TEXT,
            highest REAL,
            lowest REAL,
            order_count INTEGER,
            volume INTEGER,
            regionID TEXT,
            typeID TEXT,
            extracted_timestamp TEXT
        );
        CREATE INDEX ix_order_history_index ON order_history (index);
    '''
    try:
        cursor.execute(command)
        db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        db.rollback()
        cursor.close()
        return 1
    cursor.close()


'''
Pull all public stations
'''
def fetch_stations():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/staStations.csv'  
    try:
        data = pd.read_csv(url)
        data.drop(['x','y','z'], axis=1, inplace=True)
        execute_values(db, data, 'stations')
    except db.Error as er:
        return er

"""
Pull all types - items
"""
def fetch_types():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/invTypes.csv'
    try:
        data = pd.read_csv(url)
        execute_values(db, data, 'types')
    except db.Error as er:
        return er

"""
pull all regions
"""
def fetch_regions():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/mapRegions.csv'
    
    try:
        data = pd.read_csv(url)
        data.drop(['x','y','z','xMin','xMax','yMin','yMax','zMin','zMax'], axis=1, inplace=True)
        execute_values(db, data, 'regions')
    except db.Error as er:
        return er

"""
pull all systems
"""
def fetch_systems():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/mapSolarSystems.csv'
    
    try:
        data = pd.read_csv(url)
        data.drop(['x','y','z','xMin','xMax','yMin','yMax','zMin','zMax'], axis=1, inplace=True)
        execute_values(db, data, 'systems')
    except db.Error as er:
        return er

"""
Using a manual list of structures as these are extremely difficult to work with directly from ESI
They require you to know the struct ID and also have acces rights in-game. 
Mass-pulling without having access rights will begin to return 400 errors
and the ESI error rate will trip, meaning no more queries.
"""
def insert_known_structures():
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute("""DELETE FROM structures""")
        cur.execute("""INSERT INTO structures (structureID, regionID, structureName)
                        VALUES (1039149782071,10000023, 'F-NMX6 - Mothership Bellicose')""")
        db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        db.rollback()
        cur.close()
        return 1
    cur.close()

"""
CLI commands
"""
@click.command('fetch-systems')
def fetch_systems_command():
    resp = fetch_systems()
    if resp is None:
        click.echo('Fetched systems.')
    else:
        click.echo(resp)

@click.command('fetch-regions')
def fetch_regions_command():
    resp = fetch_regions()
    if resp is None:
        click.echo('Fetched regions.')
    else:
        click.echo(resp)

@click.command('fetch-stations')
def fetch_stations_command():
    resp = fetch_stations()
    if resp is None:
        click.echo('Fetched stations.')
    else:
        click.echo(resp)

@click.command('fetch-types')
def fetch_types_command():
    resp = fetch_types()
    if resp is None:
        click.echo('Fetched types.')
    else:
        click.echo(resp)

@click.command('fetch-all')
def fetch_all_command():
    resp_reg = fetch_regions()
    resp_sys = fetch_systems()
    resp_sta = fetch_stations()
    resp_typ = fetch_types()
    resp_struct = insert_known_structures()
    error = None
    for i in [resp_reg, resp_sta, resp_sys, resp_typ, resp_struct]:
        if i != None:
            error = 'error'

    if error == None:
        click.echo('Fetched All.')
    else:
        click.echo('Failed to fetch all')
        for i in [resp_reg, resp_sta, resp_sys, resp_typ, resp_struct]:
            if i != None:
                click.echo(i)
        


@click.command('init-db')
def init_db_command():
    '''Clear the existing data and create new tables.'''
    init_db()
    click.echo('Initialized the database.')


def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(fetch_stations_command)
    app.cli.add_command(fetch_types_command)
    app.cli.add_command(fetch_regions_command)
    app.cli.add_command(fetch_systems_command)
    app.cli.add_command(fetch_all_command)