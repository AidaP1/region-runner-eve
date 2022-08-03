from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from datetime import datetime
from region_runner.db import get_db
from region_runner.get_esi.get_concurrent import get_concurrent_reqs

bp = Blueprint('dataview', __name__, url_prefix='/dataview')

@bp.route('/search', methods=('GET', 'POST'))
def search():
    if request.method == 'POST':
        from_system = request.form['from_system']
        to_system = request.form['to_system']
        db = get_db()
        error = None

        if not from_system:
            error = '"From" system is required.'
        elif not to_system:
            error = '"To" system is required.'

        orders = get_concurrent_reqs()
        date_today = str(datetime.now)
        if orders:
            for order in orders:
                try:
                    db.executemany(
                        "INSERT INTO orders (duration, is_buy_order, issued, station_id, min_volume, order_id, price, order_range, typeid, volume_remaining, total_volume, extracted_timestamp) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (order["duration"],order["is_buy_order"], order["issued"], order["location_id"], order["min_volume"], order["order_id"], order["price"], order["range"], order["type_id"], order["volume_remain"], order["volume_total"], date_today)
                    )
                    db.commit()
                except db.Error as er:
                    error = er
                
        if error is None:
                return render_template('/dataview/search.html', orders=orders)

        flash(error)

    return render_template('/dataview/search.html')