from requests.auth import HTTPBasicAuth
import grequests, requests, os, datetime
from common import cache
from region_runner.db import get_db
import pandas as pd
import itertools as it
import click

# refresh access token with API key
def get_access_token():
    url = 'https://login.eveonline.com/v2/oauth/token'
    call_time = datetime.datetime.now()
    client_id = os.environ['CLIENT_ID']
    client_secret = os.environ['CLIENT_SECRET']
    refresh_token = os.environ['REFRESH_TOKEN']
    # refresh_token = cache.get('refresh_token') or os.environ['REFRESH_TOKEN']

    response = requests.post(
        url,
        data={'grant_type':'refresh_token', 'refresh_token': refresh_token},
        headers={'Content-Type': 'application/x-www-form-urlencoded', 
                'Host':'login.eveonline.com'},
        auth=HTTPBasicAuth(client_id, client_secret)).json()

    token_expiry = call_time + datetime.timedelta(seconds=response['expires_in'])
    refresh_token = response['refresh_token']
    cache.set("token_expiry", token_expiry)
    cache.set("refresh_token", refresh_token)
    cache.set("access_token", response["access_token"])
    return response["access_token"]

# pulling player-owned structs
def concurrent_structure_requests(pages, url, access_token):
    reqs = []
    for page in range(2, pages + 1):
        req = grequests.get(
            url, 
            params={'page': page}, 
            headers={'Authorization': 'Bearer ' + access_token})
        reqs.append(req)

    responses = grequests.map(reqs)
    return responses

def get_concurrent_structures(url, access_token):
    all_orders = []
    req = grequests.get(
        url, 
        headers={'Authorization': 'Bearer ' + access_token}).send()
    res = req.response

    res.raise_for_status()

    all_orders.extend(res.json())
    pages = int(res.headers['X-Pages'])
    responses = concurrent_structure_requests(pages, url, access_token)

    for response in responses:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print('Received status code {} from {}'.format(response.status_code, response.url))
            continue

        data = response.json()
        all_orders.extend(data)
    
    return all_orders

def get_structure_data(id):
    if cache.get("token_expiry") is None or cache.get("access_token") is None:
        access_token = get_access_token()
    elif datetime.datetime.now() > cache.get("token_expiry"):
        access_token = get_access_token()
    else:
        access_token = cache.get("access_token")

    print(cache.get('refresh_token'))
    url = "https://esi.evetech.net/latest/markets/structures/" +str(id)
    resp = get_concurrent_structures(url, access_token)
    return resp

def fetch_all_structures():
    db = get_db()
    structs_to_pull = db.execute('SELECT id FROM structures').fetchall()
    for struct in structs_to_pull:
        orders = get_structure_data(struct['id'])
        date_time = str(datetime.datetime.now())
        if orders:
            df = pd.DataFrame(orders)
            df = df.assign(extracted_timestamp=date_time)
            df.to_sql('orders', con=db, if_exists='append')


# pulling public region data
def get_region_data(id):
    url = "https://esi.evetech.net/latest/markets/"+str(id)+"/orders/"
    resp = get_concurrent_regions(url)
    return resp

def concurrent_region_requests(pages, url):
    reqs = []
    for page in range(2, pages + 1):
        req = grequests.get(url, params={'page': page})
        reqs.append(req)

    responses = grequests.map(reqs)
    return responses

def get_concurrent_regions(url):
    all_orders = []
    req = grequests.get(url).send()
    res = req.response

    res.raise_for_status()

    all_orders.extend(res.json())
    pages = int(res.headers['X-Pages'])
    responses = concurrent_region_requests(pages, url)

    for response in responses:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print('Received status code {} from {}'.format(response.status_code, response.url))
            continue

        data = response.json()
        all_orders.extend(data)
    
    return all_orders

def fetch_all_regions():
    db = get_db()
    regions_to_pull = db.execute("""SELECT regionID FROM regions""").fetchall()
    for region in regions_to_pull:
        orders = get_region_data(region['regionId'])
        date_time = str(datetime.datetime.now())
        if orders:
            df = pd.DataFrame(orders)
            df = df.assign(extracted_timestamp=date_time)
            df.to_sql('orders', con=db, if_exists='append')




# CLI commands
@click.command('fetch-region-orders')
def fetch_all_regions_command():
    fetch_all_regions()
    click.echo('Fetched all public region orders.')

@click.command('fetch-struct-orders')
def fetch_all_structures_command():
    fetch_all_structures()
    click.echo('Fetched all player structure orders.')

@click.command('fetch-all-orders')
def fetch_all_orders_command():
    fetch_all_structures()
    fetch_all_regions()
    click.echo('Fetched all orders.')



def init_app(app):
    app.cli.add_command(fetch_all_regions_command)
    app.cli.add_command(fetch_all_structures_command)
    app.cli.add_command(fetch_all_orders_command)
    