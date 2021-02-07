from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install
import os
import json

USERDATAPATH = os.path.abspath(os.path.join(os.path.expanduser('~'), '.metacatalog', 'user_data'))
CONFIGFILE = os.path.join(os.path.expanduser('~'), '.metacatalog', 'config.json')


def requirements():
    with open('requirements.txt') as f:
        return f.read().split('\n')


def version():
    with open('VERSION') as f:
        return f.read().strip()


def readme():
    with open('README.md') as f:
        return f.read()


def create_config_file():
    if not os.path.exists(os.path.dirname(CONFIGFILE)):
        os.mkdir(os.path.dirname(CONFIGFILE))
    if not os.path.exists(USERDATAPATH):
        os.mkdir(USERDATAPATH)
    if not os.path.exists(CONFIGFILE):
        with open(CONFIGFILE, 'w') as f:
            json.dump(dict(), f, indent=4)


def migrate_database():
    """
    This can fail if the install command is run for the fist time.
    In these cases, a database migration is not necessary, as the 
    latest Models will be installed anyway.
    """
    try:
        from metacatalog import api
        from metacatalog.db import migration
    except ModuleNotFoundError:
        # this happens on first startup as the dependencies are 
        # not yet installed. Can be ignored.
        pass

    try:
        session = api.connect_database()
        migration.upgrade(session)
    except:
        print()
        pass


class PostDevelopCommand(develop):
    def run(self):
        create_config_file() 
        migrate_database()    
        develop.run(self)


class PostInstallCommand(install):
    def run(self):
        create_config_file()
        migrate_database()
        install.run(self)


setup(
    name="metacatalog",
    author="Mirko Mälicke",
    author_email="mirko.maelicke@kit.edu",
    license="GPL v3",
    install_requires=requirements(),
    version=version(),
    description="Metadata model management module.",
    long_description=readme(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    scripts=['metacatalog/metacatalog'],
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand
    },
    entrypoints={
        'console_scripts': ['metacatalog = metacatalog.command_line:main']
    },
    include_package_data=True,
    zip_safe=False
)
