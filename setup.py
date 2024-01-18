from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install

def requirements():
    with open('requirements.txt') as f:
        return f.read().split('\n')


def version():
    with open('metacatalog/__version__.py') as f:
        c = f.read()

    d = dict()
    exec(c, d, d)
    return d['__version__']


def readme():
    with open('README.md') as f:
        return f.read()


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
        # create config and migrate the database
        migrate_database()

        # default develop
        develop.run(self)


class PostInstallCommand(install):
    def run(self):
        # create config and migrate the database
        migrate_database()

        # default install
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
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand
    },
    entry_points={
        'console_scripts': [
            'metacatalog = metacatalog.command_line:main'
        ]
    },
    include_package_data=True,
    zip_safe=False
)
