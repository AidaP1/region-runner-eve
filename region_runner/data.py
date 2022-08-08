from flask import (
    Blueprint, flash, g, current_app, render_template, request
)
from datetime import datetime
from region_runner.db import get_db
from region_runner.get_esi.get_markets import get_region_data
import pandas as pd

bp = Blueprint('data', __name__, url_prefix='/data')

@bp.route('/get-data', methods=('GET', 'POST'))
def get_data():
    db = get_db()
    regions = db.execute("""SELECT * FROM regions""").fetchall()
    if request.method == 'POST':
        regions_to_pull = []
        for region in request.form:
            region_name = request.form[region]
            region_id = db.execute('SELECT regionID FROM regions WHERE regionName = ?', (region_name,)).fetchone()['regionID']
            regions_to_pull.append(region_id)  

        for region in regions_to_pull:
            orders = get_region_data(region)
            date_time = str(datetime.now())
            if orders:
                df = pd.DataFrame(orders)
                df = df.assign(extracted_timestamp=date_time)
                df.to_sql('orders', con=db, if_exists='append')

    return render_template('/data/get-data.html', regions=regions)


@bp.route('/show-data', methods=('GET', 'POST'))
def show_data():
    db = get_db()
    stations = db.execute("""SELECT * FROM stations""").fetchall()

    if request.method == 'POST':
        error = None
        with current_app.open_resource('static/query/dynam_arb_query.sql') as f:
            query = f.read().decode('utf8')

        from_id = 60003760 # request.form['from_address'] 
        to_id = 60008494 # request.form['to_address']

        if not from_id:
            error = 'Missing from_id'
        if not to_id:
            error = 'Missing to_id'

        try:    
            res = db.execute(query.format(from_id, to_id)).fetchall()
        except db.Error as er:
            error = er

        if error == None:
            orders = []
            for r in res:
                o = {}
                o['typeid'] = r['typeid']
                o['buy_price'] = r['buy_price']
                o['sell_price'] = r['sell_price']
                o['base_margin'] = r['base_margin']
                orders.append(o)

            return render_template('/data/show-data.html', stations= stations, orders=orders)

        flash(error)

    return render_template('/data/show-data.html', stations= stations)