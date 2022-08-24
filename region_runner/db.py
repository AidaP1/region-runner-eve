
from dataclasses import dataclass
import psycopg2, psycopg2.extras, click, os
import pandas as pd
from flask import current_app, g

def get_db():
    if 'db' not in g:
        # pulls from heroku 
        DATABASE_URL = os.environ['DATABASE_URL']
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        g.db = psycopg2.connect(
            DATABASE_URL, 
            sslmode='require')

    return g.db

"""
Gets the data into the DB
"""
def execute_values(conn, df, table):
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_db():
    db = get_db()
    cursor = db.cursor()
    command = """
        DROP TABLE IF EXISTS structures;
        DROP TABLE IF EXISTS systems;
        DROP TABLE IF EXISTS regions;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS types;
        DROP TABLE IF EXISTS order_history;

        CREATE TABLE `staStations` (
            `stationID` bigint(20) NOT NULL,
            `security` double DEFAULT NULL,
            `dockingCostPerVolume` double DEFAULT NULL,
            `maxShipVolumeDockable` double DEFAULT NULL,
            `officeRentalCost` int(11) DEFAULT NULL,
            `operationID` int(11) DEFAULT NULL,
            `stationTypeID` int(11) DEFAULT NULL,
            `corporationID` int(11) DEFAULT NULL,
            `solarSystemID` int(11) DEFAULT NULL,
            `constellationID` int(11) DEFAULT NULL,
            `regionID` int(11) DEFAULT NULL,
            `stationName` varchar(100) DEFAULT NULL,
            `x` double DEFAULT NULL,
            `y` double DEFAULT NULL,
            `z` double DEFAULT NULL,
            `rep rocessingEfficiency` double DEFAULT NULL,
            `reprocessingStationsTake` double DEFAULT NULL,
            `reprocessingHangarFlag` int(11) DEFAULT NULL
        )

        CREATE TABLE "systems" (
            "index" INTEGER,
            "regionID" INTEGER,
            "constellationID" INTEGER,
            "solarSystemID" INTEGER,
            "solarSystemName" TEXT,
            "x" REAL,
            "y" REAL,
            "z" REAL,
            "xMin" REAL,
            "xMax" REAL,
            "yMin" REAL,
            "yMax" REAL,
            "zMin" REAL,
            "zMax" REAL,
            "luminosity" REAL,
            "border" INTEGER,
            "fringe" INTEGER,
            "corridor" INTEGER,
            "hub" INTEGER,
            "international" INTEGER,
            "regional" INTEGER,
            "constellation" TEXT,
            "security" REAL,
            "factionID" TEXT,
            "radius" REAL,
            "sunTypeID" TEXT,
            "securityClass" TEXT
        );

        CREATE TABLE "regions" (
            "index" INTEGER,
            "regionID" INTEGER,
            "regionName" TEXT,
            "x" REAL,
            "y" REAL,
            "z" REAL,
            "xMin" REAL,
            "xMax" REAL,
            "yMin" REAL,
            "yMax" REAL,
            "zMin" REAL,
            "zMax" REAL,
            "factionID" TEXT,
            "nebula" INTEGER,
            "radius" TEXT
        );

        CREATE TABLE "orders" (
            "index" INTEGER,
            "duration" INTEGER,
            "is_buy_order" INTEGER,
            "issued" TEXT,
            "location_id" INTEGER,
            "min_volume" INTEGER,
            "order_id" INTEGER,
            "price" REAL,
            "range" TEXT,
            "system_id" INTEGER,
            "type_id" INTEGER,
            "volume_remain" INTEGER,
            "volume_total" INTEGER,
            "extracted_timestamp" TEXT
        );

        CREATE TABLE "types" (
            "index" INTEGER,
            "typeID" INTEGER,
            "groupID" INTEGER,
            "typeName" TEXT,
            "description" TEXT,
            "mass" REAL,
            "volume" REAL,
            "capacity" REAL,
            "portionSize" INTEGER,
            "raceID" TEXT,
            "basePrice" TEXT,
            "published" INTEGER,
            "marketGroupID" TEXT,
            "iconID" TEXT,
            "soundID" TEXT,
            "graphicID" INTEGER
        );

        CREATE TABLE "order_history" (
            "index" INTEGER,
            "average" REAL,
            "date" TEXT,
            "highest" REAL,
            "lowest" REAL,
            "order_count" INTEGER,
            "volume" INTEGER,
            "regionID" TEXT,
            "typeID" TEXT,
            "extracted_timestamp" TEXT
        );
        CREATE INDEX "ix_order_history_index"ON "order_history" ("index");
    """
    try:
        cursor.execute(command)
        db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        db.rollback()
        cursor.close()
        return 1
    cursor.close()


"""
Pull all public stations
"""
def fetch_stations():
    db = get_db()
    error = None
    url = 'https://www.fuzzwork.co.uk/dump/latest/staStations.csv'  
    try:
        data = pd.read_csv(url)
        execute_values(db, data, 'structures')
    except db.Error as er:
        return er

def fetch_types():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/invTypes.csv'
    try:
        data = pd.read_csv(url)
        data.to_sql('types', con=db, if_exists='replace')
    except db.Error as er:
        return er

def fetch_regions():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/mapRegions.csv'
    
    try:
        data = pd.read_csv(url)
        data.to_sql('regions', con=db, if_exists='replace')
    except db.Error as er:
        return er

def fetch_systems():
    db = get_db()
    url = 'https://www.fuzzwork.co.uk/dump/latest/mapSolarSystems.csv'
    
    try:
        data = pd.read_csv(url)
        data.to_sql('systems', con=db, if_exists='replace')
    except db.Error as er:
        return er

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
    error = None
    for i in [resp_reg, resp_sta, resp_sys, resp_typ]:
        if i != None:
            error = 'error'

    if error == None:
        click.echo('Fetched All.')
    else:
        click.echo('Failed to fetch all')
        for i in [resp_reg, resp_sta, resp_sys, resp_typ]:
            if i != None:
                click.echo(i)
        


@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new tables."""
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