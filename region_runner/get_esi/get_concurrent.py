import grequests
from requests.exceptions import HTTPError

MARKET_URL = "https://esi.evetech.net/latest/markets/10000002/orders/"

# for testing purposes only
def get_first_page():
    req = grequests.get(MARKET_URL).send()
    res = req.response

    return res

# actual code below
def concurrent_requests(pages):
    reqs = []
    for page in range(2, pages + 1):
        req = grequests.get(MARKET_URL, params={'page': page})
        reqs.append(req)

    responses = grequests.map(reqs)


    return responses

def get_concurrent_reqs():
    all_orders = []
    req = grequests.get(MARKET_URL).send()
    res = req.response

    res.raise_for_status()

    all_orders.extend(res.json())
    pages = int(res.headers['X-Pages'])
    responses = concurrent_requests(pages)

    for response in responses:
        try:
            response.raise_for_status()
        except HTTPError:
            print('Received status code {} from {}'.format(response.status_code, response.url))
            continue

        data = response.json()
        all_orders.extend(data)
    
    return all_orders

