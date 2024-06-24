# -*- coding: utf-8 -*-

from app import make_app
from flask_cors import CORS
import psutil

app=make_app('development')
CORS(app)


if __name__=='__main__':

    app.run(host='0.0.0.0',port=5000)
    
    
