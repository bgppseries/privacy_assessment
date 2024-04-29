# -*- coding: utf-8 -*-

from app import make_app
app=make_app('development')



if __name__=='__main__':

    app.run(host='0.0.0.0',port=5000)
    
