from flask import jsonify
from celery_task.task import apptask,get_time
from app.api.test import api_test
#from flask import current_app
from celery import current_app
@api_test.route("/", methods=["GET"])
def index():
    print(current_app)
    r = apptask.delay()
    return jsonify({"status":"success"})+r
@api_test.route("/time",methods=["GET","POST"])
def tmp():
    r=get_time.delay()
    return "helloworld"