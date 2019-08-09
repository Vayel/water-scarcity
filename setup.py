import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "requirements.txt")) as f:
    install_requires = f.read().splitlines()

setup(
    name='water_scarcity',
    url='https://github.com/Vayel/water-scarcity',
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
)
