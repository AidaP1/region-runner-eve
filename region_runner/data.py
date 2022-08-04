
from flask import (
    Blueprint, flash, g, current_app, render_template, request
)
from datetime import datetime
from region_runner.db import get_db
from region_runner.get_esi.get_markets import get_region_data

bp = Blueprint('data', __name__, url_prefix='/data')

@bp.route('/get-data', methods=('GET', 'POST'))
def get_data():
    db = get_db()
    regions = db.execute("""SELECT * FROM regions""").fetchall()
    if request.method == 'POST':
        regions_to_pull = []
        for region in request.form:
            region_name = request.form[region]
            region_id = db.execute('SELECT id FROM regions WHERE name = ?', (region_name,)).fetchone()['id']
            regions_to_pull.append(region_id)  

        for region in regions_to_pull:
            error = None
            orders = get_region_data(region)
            date_time = str(datetime.now())
            if orders:
                for order in orders:
                    try:
                        db.execute(
                            """INSERT INTO orders (duration, is_buy_order, issued, station_id, min_volume, order_id, price, order_range, system_id, typeid, volume_remaining, total_volume, extracted_timestamp) 
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (order["duration"],order["is_buy_order"], order["issued"], order["location_id"], order["min_volume"], order["order_id"], order["price"], order["range"], order["system_id"], order["type_id"], order["volume_remain"], order["volume_total"], date_time)
                        )
                        db.commit()
                    except db.Error as er:
                        error = er
        
        if error is None:
                return render_template('/data/get-data.html', regions=regions)

        flash(error)

    return render_template('/data/get-data.html', regions=regions)


@bp.route('/show-data', methods=('GET', 'POST'))
def show_data():
    db = get_db()
    with current_app.open_resource('./static/query/dynamic-region-summary.sql') as f:
        query = f.read().decode('utf8')

    from_id = 60003760
    to_id = 1
    params = """AND station_id ={}""".format(from_id, to_id)
    res = db.execute(query.format(params)).fetchall()

    for row in res:
        flash(row['station_id'])
    return render_template('/data/show-data.html')