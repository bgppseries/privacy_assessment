import os

class BaseConfig:

    TODOISM_LOCALES = ['en_US', 'zh_Hans_CN']
    TODOISM_ITEM_PER_PAGE = 20

    BABEL_DEFAULT_LOCALE = TODOISM_LOCALES[0]
    SECRET_KEY = os.getenv('SECRET_KEY', 'a secret string')


class DevelopmentConfig(BaseConfig):
    DEBUG=False
    REDIS_URL = "redis://:qwb@localhost:5672/2"


class ProductionConfig(BaseConfig):
    REDIS_URL = "redis://:qwb@localhost:5672/2"
    DEBUG=True


class TestingConfig(BaseConfig):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///'
    WTF_CSRF_ENABLED = False


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig
}