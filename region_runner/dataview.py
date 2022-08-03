import functools
import grequests
import time
from requests.exceptions import HTTPError

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from region_runner.db import get_db

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

        if error is None:
                return redirect(url_for("dataview.results"))

        flash(error)

    return render_template('/dataview/search.html')