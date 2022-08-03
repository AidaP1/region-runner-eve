from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
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
        if orders:
            for order in orders:
                try:
                    db.execute(
                        "INSERT INTO fnm6_orders (duration, buy, issued, location_id, min_volume, order_id, price, order_range, typeid, volume_remaining, total_volume) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (order["duration"],order["buy"], order["issued"], order["location_id"], order["min_volume"], order["order_id"], order["price"], order["order_range"], order["typeid"], order["volume_remaining"], order["total_volume"])
                    )
                    db.commit()
                except:
                    error = "DB error"
                
        if error is None:
                return render_template('/dataview/search.html', orders=orders)

        flash(error)

    return render_template('/dataview/search.html')