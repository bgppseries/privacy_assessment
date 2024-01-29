# -*- coding: utf-8 -*-

import os
from neo4j import GraphDatabase
from flask import jsonify
from app import make_app,make_celery
from celery_task.task import apptask, get_time, start_evaluate
from data_handle_model.csv_handle import get_current_parentpath
from data_handle_model.json_handel import run_command
app=make_app()
# cel=make_celery(app)
# basedir = get_current_parentpath()





if __name__=='__main__':
    #run_command("ls -l")
    print(app)
    app.run()
