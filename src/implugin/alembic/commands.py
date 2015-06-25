import re
import sys
from configparser import ConfigParser

from alembic.command import stamp
from alembic.config import CommandLine
from alembic.config import Config
import venusian

from implugin.sqlalchemy.application import SqlAlchemyApplication
from implugin.sqlalchemy.requestable import DatabaseConnection


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
            'keys': 'generic, hatak',
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
            'formatter': 'hatak',
        }
        config['formatter_hatak'] = {
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


class InitDatabase(AlembicCommand, DatabaseConnection):

    def get_datagenerator(self):
        pass

    def get_metadata(self):
        pass

    def run_alembic(self):
        self._cache = {}
        engine = self.registry['db_engine']
        metadata = self.get_metadata()
        metadata.bind = engine

        if '--iwanttodeletedb' in sys.argv:
            print('[Impaf] Removing old database...')
            for table in reversed(metadata.sorted_tables):
                engine.execute(table.delete())

        print('[Impaf] Scanning for models...')
        scan = venusian.Scanner()
        scan.scan(
            __import__(self.module),
            ignore=[re.compile('tests$').search]
        )
        print('[Impaf] Initializing database...')
        metadata.create_all()

        generator = self.get_datagenerator()
        if generator:
            print('[Impaf] Creating fixtures...')
            generator.feed_database(self.database)
            generator.create_all()

        alembic_cfg = Config(self.paths['alembic:ini'])
        stamp(alembic_cfg, 'head')

    @property
    def registry(self):
        return self.config.registry
