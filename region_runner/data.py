from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from datetime import datetime
from region_runner.db import get_db
from region_runner.get_esi.get_concurrent import get_concurrent_reqs

bp = Blueprint('data', __name__, url_prefix='/data')

@bp.route('/get-data', methods=('GET', 'POST'))
def get_data():
    stations = db.execute("""SELECT UNIQUE name FROM stations""")

    if request.method == 'POST':
        db = get_db()
        error = None

        orders = get_concurrent_reqs()
        date_today = str(datetime.now)
        if orders:
            for order in orders:
                try:
                    db.execute(
                        """INSERT INTO orders (duration, is_buy_order, issued, station_id, min_volume, order_id, price, order_range, typeid, volume_remaining, total_volume, extracted_timestamp) 
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (order["duration"],order["is_buy_order"], order["issued"], order["location_id"], order["min_volume"], order["order_id"], order["price"], order["range"], order["type_id"], order["volume_remain"], order["volume_total"], date_today)
                    )
                    db.commit()
                except db.Error as er:
                    error = er
                
        if error is None:
                return render_template('/data/get-data.html', stations=stations)

        flash(error)

    return render_template('/data/get-data.html', stations=stations)