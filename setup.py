# -*- encoding: utf-8 -*-
from setuptools import setup, find_packages

install_requires = [
    'impaf',
    'alembic',
]

if __name__ == '__main__':
    setup(
        name='impaf-alembic',
        version='0.1',
        description='Alembic plugin for Impaf.',
        license='Apache License 2.0',
        packages=find_packages('src'),
        package_dir={'': 'src'},
        namespace_packages=['implugin'],
        install_requires=install_requires,
        include_package_data=True,
        zip_safe=False,
    )
