from requests.auth import HTTPBasicAuth
import grequests, requests, os, datetime
from common import cache

def get_access_token():
    url = 'https://login.eveonline.com/v2/oauth/token'
    call_time = datetime.datetime.now()
    client_id = os.environ['CLIENT_ID']
    client_secret = os.environ['CLIENT_SECRET']
    refresh_token = cache.get("refresh_token") or os.environ['REFRESH_TOKEN']
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

def concurrent_structure_requests(pages, url, access_token):
    reqs = []
    for page in range(2, pages + 1):
        req = grequests.get(url, params={'page': page}, header={'Authorization': 'Bearer ' + access_token})
        reqs.append(req)

    responses = grequests.map(reqs)
    return responses

def get_concurrent_structures(url, access_token):
    all_orders = []
    req = grequests.get(url, header={'Authorization': 'Bearer ' + access_token}).send()
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
        access_token = cache.get("token_expiry")

    url = "https://esi.evetech.net/latest/markets/structures/1039149782071" # using mothership B to test +str(id)
    resp = get_concurrent_structures(url, access_token)
    return resp


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

