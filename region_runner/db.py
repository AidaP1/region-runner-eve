
from dataclasses import dataclass
import os
import psycopg2
import click
import pandas as pd
from flask import current_app, g

def get_db():
    if 'db' not in g:
        DATABASE_URL = os.environ['DATABASE_URL']
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        g.db = psycopg2.connect(
            DATABASE_URL, 
            sslmode='require')

    return g.db

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
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
    with current_app.open_resource('./schema.sql') as f:
        db.executescript(f.read().decode('utf8'))

def fetch_stations():
    db = get_db()
    error = None
    url = 'https://www.fuzzwork.co.uk/dump/latest/staStations.csv'
    
    try:
        data = pd.read_csv(url)
        execute_values(db, data, 'structures')
        # data.to_sql('stations', con=g.conn, if_exists='replace')
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