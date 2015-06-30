import re
import sys
import logging
from configparser import ConfigParser
from importlib import import_module

from alembic.command import stamp
from alembic.config import CommandLine
from alembic.config import Config
from venusian import Scanner

from implugin.sqlalchemy.application import SqlAlchemyApplication
from implugin.sqlalchemy.requestable import DatabaseConnection

log = logging.getLogger(__name__)
logging.basicConfig(level=20)


class AlembicCommand(SqlAlchemyApplication):

    def generate_alembic_config(self):
        config = ConfigParser()
        config['alembic'] = {
            'script_location': self.paths['alembic']['versions'],
            'sqlalchemy.url': self.settings['db']['url'],
        }
        config['loggers'] = {
            'keys': 'root,sqlalchemy,alembic',
        }
        config['handlers'] = {
            'keys': 'console',
        }
        config['formatters'] = {
            'keys': 'generic, impaf',
        }
        config['logger_root'] = {
            'level': 'WARN',
            'handlers': 'console',
            'qualname': '',
        }
        config['logger_sqlalchemy'] = {
            'level': 'WARN',
            'handlers': '',
            'qualname': 'sqlalchemy.engine',
        }
        config['logger_alembic'] = {
            'level': 'INFO',
            'handlers': '',
            'qualname': 'alembic',
        }
        config['handler_console'] = {
            'class': 'StreamHandler',
            'args': '(sys.stderr,)',
            'level': 'NOTSET',
            'formatter': 'impaf',
        }
        config['formatter_impaf'] = {
            'format': '[Alembic] %(message)s',
        }

        with open(self.paths['alembic:ini'], 'w') as configfile:
            config.write(configfile)
            configfile.write(
                '\n'.join([
                    '[formatter_generic]',
                    'datefmt = %H:%M:%S',
                    'format = %(levelname)-5.5s [%(name)s] %(message)s',
                ])
            )

    def set_sys_argv(self):
        sys.argv.insert(1, '-c')
        sys.argv.insert(2, self.paths['alembic']['ini'])
        if 'init' in sys.argv:
            sys.argv.append(self.paths['alembic']['versions'])

    def run_alembic(self):
        CommandLine().main()

    def run_command(self, settings={}):
        super().run_command(settings)
        self.generate_alembic_config()
        self.set_sys_argv()
        self.run_alembic()


class InitDatabase(SqlAlchemyApplication, DatabaseConnection):

    def get_datagenerator(self):
        pass

    def get_metadata(self):
        pass

    def run_command(self, settings={}):
        super().run_command(settings)
        self._init()
        self._delete_database()
        self._scan_for_models()
        self._create_schema()
        self._generate_fixtures()
        self._stamp()

    def _init(self):
        self._cache = {}
        self.engine = self.registry['db_engine']
        self.metadata = self.get_metadata()
        self.metadata.bind = self.engine
        self.log = log

    def _delete_database(self):
        if '--iwanttodeletedb' in sys.argv:
            self.log.info('Removing old database...')
            for table in reversed(self.metadata.sorted_tables):
                self.engine.execute(table.delete())

    def _scan_for_models(self):
        self.log.info('Scanning for models...')
        scan = Scanner()
        scan.scan(
            import_module(self.module),
            ignore=[re.compile('tests$').search]
        )

    def _create_schema(self):
        self.log.info('Initializing database...')
        self.metadata.create_all()

    def _generate_fixtures(self):
        generator = self.get_datagenerator()
        if generator:
            self.log.info('Creating fixtures...')
            generator.feed_database(self.database)
            generator.create_all()

    def _stamp(self):
        alembic_cfg = Config(self.paths['alembic:ini'])
        stamp(alembic_cfg, 'head')

    @property
    def registry(self):
        return self.config.registry

