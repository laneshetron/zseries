from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='zseries',
    version='0.0.1',
    url='https://github.com/laneshetron/zseries.git',
    packages=['zseries'],
    install_requires=requirements)
