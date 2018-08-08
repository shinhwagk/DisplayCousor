from setuptools import setup, find_packages

with open("VERSION")as f:
    version = f.read()

setup(
    name="xplan",
    version=version,
    packages=find_packages(),
    install_requires=['cx_Oracle'],
    author='shinhwagk',
    author_email='shinhwagk@outlook.com',
)
