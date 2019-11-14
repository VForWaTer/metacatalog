from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install
import os

CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.metacatalog', 'config.json')


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
    if not os.path.exists(os.path.dirname(CONFIG_FILE)):
        os.mkdir(os.path.dirname(CONFIG_FILE))


class PostDevelopCommand(develop):
    def run(self):
        create_config_file()     
        develop.run(self)


class PostInstallCommand(install):
    def run(self):
        create_config_file()
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