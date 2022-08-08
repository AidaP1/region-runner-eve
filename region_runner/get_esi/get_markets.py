import grequests
from requests.exceptions import HTTPError


def get_structure_data(id):
    url = "https://esi.evetech.net/latest/markets/structures/"+str(id)
    resp = get_concurrent_reqs(url)
    return resp

def get_region_data(id):
    url = "https://esi.evetech.net/latest/markets/"+str(id)+"/orders/"
    resp = get_concurrent_regions(url)
    return resp

# actual code below
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
        except HTTPError:
            print('Received status code {} from {}'.format(response.status_code, response.url))
            continue

        data = response.json()
        all_orders.extend(data)
    
    return all_orders

