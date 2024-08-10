import os
import logging
import traceback
# from flask_mongoengine import MongoEngine
from mongoengine import connect, disconnect
from app.conf import app_config


logger = logging.getLogger(__name__)


# def db_init(app):
#     db.init_app(app)


class DBWatcher(object):

    def __init__(self):
        self.db_conn = None

    def initialize(self):
        logger.info(f'DBWatcher - Initializing')
        # db preset

    def get_db_connection(self):
        if self.db_conn:
            pass
        else:
            self.connect_to_db()
        return self.db_conn

    def connect_to_db(self, alias='default'):
        logger.info(f'Opening database connection with alias {alias} in process {os.getpid()}')
        # print env
        env_vars = dict(os.environ)  # 将环境变量转换为字典

        if not app_config.MONGODB_SETTINGS["username"] and app_config.MONGODB_SETTINGS["password"]:
            logger.error('MongoDB AUTH CONFIG NOT FOUND')
        try:
            conn = connect(db=app_config.MONGODB_SETTINGS["db"],
                           host=app_config.MONGODB_SETTINGS["host"],
                           port=app_config.MONGODB_SETTINGS["port"],
                           username=app_config.MONGODB_SETTINGS["username"],
                           password=app_config.MONGODB_SETTINGS["password"],
                           authentication_source='admin',
                           alias=alias)
            if conn is None:
                pass
            self.db_conn = conn
        except Exception as e:
            msg_text = f'Encountered exception while trying to establish MongoDB connection: \r\n' \
                       f'{traceback.format_exception(e)}'
            # daily_report_maker.add_content('summary', msg_text)
            logger.error(msg_text)

    @staticmethod
    def disconnect_from_db(alias='default'):
        logger.info(f'Disconnecting database connection with alias {alias} in process {os.getpid()}')
        disconnect(alias=alias)


db_watcher = DBWatcher()
