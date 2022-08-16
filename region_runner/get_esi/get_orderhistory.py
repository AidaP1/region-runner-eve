from requests.exceptions import HTTPError
from flask import current_app
import grequests, datetime
from region_runner.db import get_db
import pandas as pd
import itertools as it
import click


# get order history
def get_concurrent_orderhistory(pairs):
    orders = []
    reqs = []
    url = 'https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}'
    for p in pairs.itertuples(index=False):
        reqs.append(url.format(p.regionID, p.type_id))
    rs = (grequests.get(r) for r in reqs)
    print('Prepped request at: {}'.format(datetime.datetime.now()))
    print('reqs length: {}'.format(len(reqs)))
    responses = grequests.map(rs)
    print('Got responses at: {}'.format(datetime.datetime.now()))
    for response in responses:
        error = None
        try:
            response.raise_for_status()
        except HTTPError:
            error = response.status_code
            print('Received status code {} from {}'.format(response.status_code, response.url))
            continue
        except:
            print('Some other error occured with the ESI request')
            continue

        
        if error == None:
            data = response.json()
            regionID = response.request.url[39:47]
            typeID = response.request.url[89:]
            for d in data:
                d['regionID'] = regionID
                d['typeID'] = typeID
            orders.extend(data)

    return orders

# requires a list of regions and all type id's for that region
def fetch_order_history():
    print('No Limit')
    print('Started at: {}'.format(datetime.datetime.now()))
    db = get_db()
    with current_app.open_resource('static/query/orderhist-to-pull.sql') as f:
            query = f.read().decode('utf8')
    pairs = pd.read_sql_query(query, db)
    print('Got pairs at: {}'.format(datetime.datetime.now()))

    date_time = str(datetime.datetime.now())
    orders = get_concurrent_orderhistory(pairs)
    
    #try:
    df = pd.DataFrame(orders)
    df = df.assign(extracted_timestamp=date_time)
    print('Final DF at: {}'.format(datetime.datetime.now()))
    print(df)
    df.to_sql('order_history', con=db, if_exists='append')
    print('Finished at: {}'.format(datetime.datetime.now()))
    #except :
    #    print('Failed to copy to Dataframe')

#add to cli
@click.command('fetch-order-hist')
def fetch_order_history_command():
    fetch_order_history()
    click.echo('Fetched order history.')

def init_app(app):
    app.cli.add_command(fetch_order_history_command)