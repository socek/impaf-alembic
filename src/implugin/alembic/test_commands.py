import os
from tempfile import NamedTemporaryFile
from mock import DEFAULT
from mock import MagicMock
from mock import patch
from mock import sentinel

from pytest import fixture
from pytest import yield_fixture

from implugin.sqlalchemy.application import SqlAlchemyApplication

from .commands import AlembicCommand
from .commands import InitDatabase
from .commands import log


class MockedAlembicCommand(SqlAlchemyApplication):

    def run_command(self, settings):
        self._runned = True


class ExampleAlembicCommand(AlembicCommand, MockedAlembicCommand):
    pass


class LocalFixtures(object):

    @yield_fixture
    def msys(self):
        patcher = patch('implugin.alembic.commands.sys')
        with patcher as mock:
            yield mock


class TestAlembicCommand(LocalFixtures):

    @fixture
    def command(self):
        return ExampleAlembicCommand('module')

    @yield_fixture
    def mCommandLine(self):
        patcher = patch('implugin.alembic.commands.CommandLine')
        with patcher as mock:
            yield mock

    @yield_fixture
    def mgenerate_alembic_config(self, command):
        patcher = patch.object(command, 'generate_alembic_config')
        with patcher as mock:
            yield mock

    @yield_fixture
    def mset_sys_argv(self, command):
        patcher = patch.object(command, 'set_sys_argv')
        with patcher as mock:
            yield mock

    @yield_fixture
    def mrun_alembic(self, command):
        patcher = patch.object(command, 'run_alembic')
        with patcher as mock:
            yield mock

    def test_generate_alembic_config(self, command):
        fp = NamedTemporaryFile(delete=False)
        command.paths = {
            'alembic': {
                'versions': '/tmp/versions',
            },
            'alembic:ini': fp.name,
        }
        command.settings = {
            'db': {
                'url': 'sqlite://url'
            }
        }
        command.generate_alembic_config()

        assert os.stat(fp.name).st_size > 0

    def test_set_sys_argv(self, command, msys):
        msys.argv = ['alembic']
        command.paths = {
            'alembic': {
                'ini': '/tmp/myini',
            },
        }

        command.set_sys_argv()

        assert msys.argv == ['alembic', '-c', '/tmp/myini']

    def test_set_sys_argv_with_init(self, command, msys):
        msys.argv = ['alembic', 'init']
        command.paths = {
            'alembic': {
                'ini': '/tmp/myini',
                'versions': '/tmp/versions'
            },
        }

        command.set_sys_argv()

        assert msys.argv == [
            'alembic',
            '-c',
            '/tmp/myini',
            'init',
            '/tmp/versions'
        ]

    def test_run_alembic(self, command, mCommandLine):
        command.run_alembic()

        mCommandLine.assert_called_once_with()
        mCommandLine.return_value.main.assert_called_once_with()

    def test_run_command(
        self,
        command,
        mgenerate_alembic_config,
        mset_sys_argv,
        mrun_alembic,
    ):
        command.run_command({'settings': None})

        assert command._runned is True
        mgenerate_alembic_config.assert_called_once_with()
        mset_sys_argv.assert_called_once_with()
        mrun_alembic.assert_called_once_with()


class MockedInitDatabase(SqlAlchemyApplication):

    def run_command(self, settings):
        self._runned = True


class ExampleInitDatabase(InitDatabase, MockedInitDatabase):

    def __init__(self):
        super().__init__('module')
        self._generator = MagicMock()
        self._driver = MagicMock()

    def get_datagenerator(self):
        super().get_datagenerator()
        return self._generator

    def get_driver(self):
        super().get_driver()
        return self._driver


class TestInitDatabase(LocalFixtures):

    @fixture
    def command(self):
        return ExampleInitDatabase()

    @yield_fixture
    def mstamp(self):
        patcher = patch('implugin.alembic.commands.stamp')
        with patcher as mock:
            yield mock

    @yield_fixture
    def mConfig(self):
        patcher = patch('implugin.alembic.commands.Config')
        with patcher as mock:
            yield mock

    @yield_fixture
    def m_init(self, command):
        patcher = patch.object(command, '_init')
        with patcher as mock:
            yield mock

    @yield_fixture
    def m_delete_database(self, command):
        patcher = patch.object(command, '_delete_database')
        with patcher as mock:
            yield mock

    @yield_fixture
    def m_collect_metadatas(self, command):
        patcher = patch.object(command, '_collect_metadatas')
        with patcher as mock:
            yield mock

    @yield_fixture
    def m_create_schema(self, command):
        patcher = patch.object(command, '_create_schema')
        with patcher as mock:
            yield mock

    @yield_fixture
    def m_generate_fixtures(self, command):
        patcher = patch.object(command, '_generate_fixtures')
        with patcher as mock:
            yield mock

    @yield_fixture
    def m_stamp(self, command):
        patcher = patch.object(command, '_stamp')
        with patcher as mock:
            yield mock

    def test_init(self, command):
        command.config = MagicMock()
        engine = MagicMock()
        command.config.registry = {'db_engine': engine}
        command._init()

        assert command._cache == {}
        assert command.engine == engine
        assert command.metadatas == set()
        assert command.log == log

    def test_delete_database_when_no_password(self, command, msys):
        msys.argv = []
        command.log = MagicMock()

        command._delete_database()

        assert command.log.info.called is False

    def test_delete_database(self, command, msys):
        msys.argv = ['--iwanttodeletedb']
        command.log = MagicMock()
        metadata = MagicMock()
        command.metadatas = [metadata]
        table = MagicMock()
        metadata.sorted_tables = [table]
        command.engine = MagicMock()

        command._delete_database()

        command.log.info.assert_called_once_with('Removing old database...')
        command.engine.execute.assert_called_once_with(
            table.delete.return_value)
        table.delete.assert_called_once_with()

    def test_create_schema(self, command):
        command.log = MagicMock()
        metadata = MagicMock()
        command.metadatas = [metadata]
        command.engine = MagicMock()

        command._create_schema()

        command.log.info.assert_called_once_with('Initializing database...')
        metadata.create_all.assert_called_once_with()
        assert metadata.bind == command.engine

    def test_stamp(self, command, mConfig, mstamp):
        command.paths = {'alembic:ini': 'ini'}

        command._stamp()

        mConfig.assert_called_once_with('ini')
        mstamp.assert_called_once_with(mConfig.return_value, 'head')

    def test_collect_metadatas(self, command):
        command.config = MagicMock()
        engine = MagicMock()
        db = MagicMock()
        driver = MagicMock()
        command.log = MagicMock()
        command.config.registry = {'db_engine': engine, 'db': db}
        command._driver.return_value._drivers = [driver]
        command._driver.side_effect = (
            lambda method: method() == db and DEFAULT
        )
        command.metadatas = sentinel.metadatas

        command._collect_metadatas()

        command.log.info.assert_called_once_with('Scanning for models...')
        driver._append_metadata.assert_called_once_with(sentinel.metadatas)

    def test_generate_fixtures(self, command):
        command.log = MagicMock()
        command.database = sentinel.database

        command._generate_fixtures()

        command.log.info.assert_called_once_with('Creating fixtures...')
        command._generator.feed_database.assert_called_once_with(
            sentinel.database
        )
        command._generator.create_all.assert_called_once_with()

    def test_generate_fixtures_when_no_generator_found(self, command):
        command._generator = False

        command._generate_fixtures()

    def test_run_command(
        self,
        command,
        m_init,
        m_delete_database,
        m_collect_metadatas,
        m_create_schema,
        m_generate_fixtures,
        m_stamp,
    ):
        command.run_command({})

        assert command._runned is True
        m_init.assert_called_once_with()
        m_delete_database.assert_called_once_with()
        m_collect_metadatas.assert_called_once_with()
        m_create_schema.assert_called_once_with()
        m_generate_fixtures.assert_called_once_with()
        m_stamp.assert_called_once_with()
