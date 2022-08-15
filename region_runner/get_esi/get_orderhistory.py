from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import grequests, requests, os, datetime
from common import cache
from region_runner.db import get_db
import pandas as pd
import itertools as it
import click


# get order history
def concurrent_orderhistory_requests(pages, url):
    reqs = []
    for page in range(2, pages + 1):
        req = grequests.get(
            url, 
            params={'page': page})
        reqs.append(req)

    responses = grequests.map(reqs)
    return responses

def get_concurrent_orderhistory(regionId, typeId):

    
    orders = []
    error = None

    url = 'https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}'
    reqs = []
    urls = []
    try:
        .format(regionId, typeId)
        req = grequests.get(
            url).send()
        res = req.response
        res.raise_for_status()
        error = None
    except HTTPError as er:
        error = er

    if error == None:
        # this doesn't work, as there is only 1 page per result.
        # best case is that we can concurrenty get all the orders - will it be too much!?
        orders.extend(res.json())
        pages = int(res.headers['X-Pages'])
        responses = concurrent_orderhistory_requests(pages, url)

        for response in responses:
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                print('Received status code {} from {}'.format(response.status_code, response.url))
                continue

            data = response.json()
            orders.extend(data)
        
        return orders

# requires a list of regions and all type id's for that region
def fetch_order_history():
    db = get_db()
    regions_sql = db.execute("""SELECT regionID FROM regions""").fetchall()
    types_sql = db.execute("""SELECT typeID FROM types""").fetchall()
    regions = [list(i) for i in regions_sql]
    types = [list(i) for i in types_sql]
    pairs = it.product(regions, types)


    orders  = []
    for p in pairs:
        order_history = get_concurrent_orderhistory(p[0][0], p[1][0])
        if order_history:
            orders.extend(order_history)

    
    date_time = str(datetime.datetime.now())
    if orders:
        df = pd.DataFrame(orders)
        df = df.assign(extracted_timestamp=date_time)
        df.to_sql('orders', con=db, if_exists='append')

#add to cli
@click.command('fetch-order-hist')
def fetch_order_history_command():
    fetch_order_history()
    click.echo('Fetched order history.')

def init_app(app):
    app.cli.add_command(fetch_order_history_command)