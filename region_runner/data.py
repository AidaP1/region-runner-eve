from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from datetime import datetime
from region_runner.db import get_db
from region_runner.get_esi.get_markets import get_region_data

bp = Blueprint('data', __name__, url_prefix='/data')

@bp.route('/get-markets', methods=('GET', 'POST'))
def get_markets():
    db = get_db()
    stations = db.execute("""SELECT name FROM stations""").fetchall()

    if request.method == 'POST':
        error = None
        region_id = 10000002
        orders = get_region_data(region_id)
        date_time = str(datetime.now)
        if orders:
            for order in orders:
                try:
                    db.execute(
                        """INSERT INTO orders (duration, is_buy_order, issued, station_id, min_volume, order_id, price, order_range, system, typeid, volume_remaining, total_volume, extracted_timestamp) 
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (order["duration"],order["is_buy_order"], order["issued"], order["location_id"], order["min_volume"], order["order_id"], order["price"], order["range"], order["system_id"], order["type_id"], order["volume_remain"], order["volume_total"], date_time)
                    )
                    db.commit()
                except db.Error as er:
                    error = er
                
        if error is None:
                return render_template('/data/get-data.html', stations=stations)

        flash(error)

    return render_template('/data/get-data.html', stations=stations)