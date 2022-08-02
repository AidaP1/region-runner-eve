import functools
import grequests
import time

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from region_runner.db import get_db

bp = Blueprint('dataview', __name__, url_prefix='/dataview')

from requests.exceptions import HTTPError

MARKET_URL = "https://esi.tech.ccp.is/v1/markets/10000042/orders/"


def concurrent_requests(pages):
    reqs = []
    start_time = time.time()

    for page in range(2, pages + 1):
        req = grequests.get(MARKET_URL, params={'page': page})
        reqs.append(req)

    responses = grequests.map(reqs)

    end_time = time.time()
    elapsed = end_time - start_time
    print('Elapsed time for {} requests was: {}'.format(len(responses), elapsed))

    return responses


if __name__ == '__main__':
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
    print('Got {:,d} orders.'.format(len(all_orders)))
