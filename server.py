# -*- coding: utf-8 -*-

import os
from neo4j import GraphDatabase
from flask import jsonify
from app import make_app
app=make_app()
# cel=make_celery(app)

from celery_task.celery import start_grouptask,subtask,start_evaluate



if __name__=='__main__':
    #run_command("ls -l")
    q=['sex','age','id_number','phone']
    idd=['uuid']
    sa=['hotel_name']
    url='mysql+pymysql://root:784512@localhost:3306/local_test'
    table_name = "local_test_hotel_1_5_2"
    un_table_name = "local_test_hotel_0_0_0"
    # result=start_evaluate.delay(url,un_table_name,url,table_name,q,sa,idd)
    # print("task_id is: ",result.id)
    app.run()
