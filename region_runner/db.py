import sqlite3
import gevent.monkey
gevent.monkey.patch_all()
import click
import requests
from flask import current_app, g, flash, render_template
import bz2


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_db():
    db = get_db()
    with current_app.open_resource('./schema.sql') as f:
        db.executescript(f.read().decode('utf8'))

def load_locations():
    db = get_db()
    with current_app.open_resource('./static/locations.sql') as f:
        db.executescript(f.read().decode('utf8'))

def fetch_stations():
    db= get_db()
    # old fizzworks pull, but the file format doesn't work on account of the 'Key' lines
    # url = 'https://www.fuzzwork.co.uk/dump/latest/staStations.sql.bz2'
    # req = bz2.decompress(requests.get(url).content).decode()
    # error = None

    try:
        db.executescript('./static/staStations.sql')
    except db.Error as er:
        error = er
        return error
    

@click.command('init-db')
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database.')

@click.command('load-locations')
def load_locations_command():
    load_locations()
    click.echo('Loaded locations.')

@click.command('fetch-stations')
def fetch_stations_command():
    resp = fetch_stations()
    if resp is None:
        click.echo('Fetched stations.')
    else:
        click.echo(resp)

def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(load_locations_command)
    app.cli.add_command(fetch_stations_command)