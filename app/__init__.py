
from flask import Flask,jsonify
# from celery import Celery
from mylog.logger import mylogger,set_logger
from logging import INFO
from app.api.file import api_file
from app.api.show import api_show
from app.config.flask_config import config
from app.api.test import api_test

###创建日志
logger=mylogger(__name__,INFO)
set_logger(logger)



def make_app(config_name=None):
    logger.info('flask app 正在创建')
    app=Flask('app')
    app.config.from_object(config[config_name])
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
    register_errors(app)
    register_blueprints(app)
    return app


#### 导入蓝图
def register_blueprints(app):
    """
        api接口
        
        file用于文件上传
        show用于前端展示，提供函数接口调用
        test为测试所用
    """
    with app.app_context():
        app.register_blueprint(api_test, url_prefix='/api/test')
        app.register_blueprint(api_file,url_prefix='/api/file')
        app.register_blueprint(api_show,url_prefix='/api/show')


def register_errors(app):
    @app.errorhandler(400)
    def bad_request(e):
        response = jsonify(code=400, message="Bad Request")
        response.status_code = 400
        return response

    @app.errorhandler(403)
    def forbidden(e):
        response = jsonify(code=403, message="Forbidden")
        response.status_code = 403
        return response

    @app.errorhandler(404)
    def page_not_found(e):
        response = jsonify(code=404, message="The requested URL was not found on the server.")
        response.status_code = 404
        return response

    @app.errorhandler(405)
    def method_not_allowed(e):
        response = jsonify(code=405, message='The method is not allowed for the requested URL.')
        response.status_code = 405
        return response
