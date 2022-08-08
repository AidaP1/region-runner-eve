import grequests, requests, os, datetime

#is this messy, mixing env variables??
def get_access_token():
    url = 'https://login.eveonline.com/oauth/token?'
    call_time = datetime.datetime.now()
    response = requests.post(
        url,
        data={"grant_type": "refresh_token", 'referesh_token': os.environ['REFRESH_TOKEN']},
        headers={'Content-Type': 'application/x-www-form-urlencoded', 
                'Host':'login.eveonline.com',
                'Authorization': 'Basic '+os.environ([''])+':'+os.environ['client_secret']}
    )
    os.environ['TOKEN_EXPIRY'] = call_time + datetime.timedelta(seconds=response['expires_in'])  # need to properly add datetimes
    os.environ['REFRESH_TOKEN'] = response['refresh_token']

    return response.json()['access_token']

def concurrent_structure_requests(pages, url, access_token):
    reqs = []
    for page in range(2, pages + 1):
        req = grequests.get(url, params={'page': page}, header={'Authorization': 'Bearer ' + access_token})
        reqs.append(req)

    responses = grequests.map(reqs)
    return responses

def get_concurrent_structures(url, access_token):
    all_orders = []
    req = grequests.get(url).send()
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
    access_req = get_access_token()
    access_token = access_req['access_token']

    url = "https://esi.evetech.net/latest/markets/structures/"+str(id)
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

