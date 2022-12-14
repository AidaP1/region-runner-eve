from flask import (
    Blueprint, flash, g, current_app, render_template, request
)
from region_runner.db import get_db

bp = Blueprint('data', __name__, url_prefix='/data')

@bp.route('/show-data', methods=('GET', 'POST'))
def show_data():
    db = get_db()
    stations = db.execute("""SELECT * FROM stations""").fetchall()
    if request.method == 'POST':
        error = None
        with current_app.open_resource('static/query/dynam_arb_query.sql') as f:
            query = f.read().decode('utf8')

        from_id = 60003760 # request.form['from_address'] 
        to_id = 1039149782071 # request.form['to_address']

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
                o['type_id'] = r['type_id']
                o['type_name'] = r['typeName']
                o['buy_price'] = r['buy_price']
                o['sell_price'] = r['sell_price']
                o['base_margin'] = r['base_margin']
                o['volume'] = r['volume']
                orders.append(o)
            return render_template('/data/show-data.html', stations= stations, orders=orders)

    

    return render_template('/data/show-data.html', stations= stations)