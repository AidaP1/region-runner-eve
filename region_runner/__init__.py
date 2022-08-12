import gevent.monkey
gevent.monkey.patch_all()

import datetime
import os

from flask import (Flask, render_template)
from common import cache
from region_runner.get_esi import get_markets



def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'region_runner.sqlite'),
    )
    cache.init_app(app=app, config={"CACHE_TYPE": "FileSystemCache",'CACHE_DIR': '/tmp'})

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/')
    def landing():
        return render_template('/home.html')


    from . import db
    db.init_app(app)

    from . import data
    app.register_blueprint(data.bp)

    from . import get_esi
    get_esi.get_markets.init_app(app)

    
    return app
