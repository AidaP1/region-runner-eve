from requests.exceptions import HTTPError
import grequests, datetime
from region_runner.db import get_db
import pandas as pd
import itertools as it
import click


# get order history
def get_concurrent_orderhistory(pairs):
    orders = []
    df = pd.DataFrame()

    url = 'https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}'
    reqs = [url.format(p[0][0], p[1][0]) for p in pairs]
    rs = (grequests.get(r) for r in reqs)
    responses = grequests.map(rs)

    for response in responses:
        print(response.json())
        try:
            response.raise_for_status()
        except HTTPError:
            print('Received status code {} from {}'.format(response.status_code, response.url))
            continue

        data = response.json()
        orders.extend(data)

        return orders

# requires a list of regions and all type id's for that region
def fetch_order_history():
    db = get_db()
    regions_sql = db.execute("""SELECT regionID FROM regions""").fetchall()
    types_sql = db.execute("""SELECT typeID FROM types WHERE published=1""").fetchall()
    regions = [list(i) for i in regions_sql]
    types = [list(i) for i in types_sql]
    pairs = it.product(regions, types)

    orders = get_concurrent_orderhistory(pairs)
    date_time = str(datetime.datetime.now())

    # refac to try/except to make clear it's an error
    try:
        df = pd.DataFrame(orders)
        df = df.assign(extracted_timestamp=date_time)
        df.to_sql('order_history', con=db, if_exists='append')
    except:
        print('Failed to copy to Dataframe')

#add to cli
@click.command('fetch-order-hist')
def fetch_order_history_command():
    fetch_order_history()
    click.echo('Fetched order history.')

def init_app(app):
    app.cli.add_command(fetch_order_history_command)