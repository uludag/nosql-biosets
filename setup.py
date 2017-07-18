from setuptools import setup, find_packages

setup(
       name='nosqlbiosets',
       version='0.0.1',
       packages=find_packages(),
       data_files=[('data', ['data/gene2pubtator.sample'])],
       test_suite='tests',
       long_description='Scripts for indexing sample bioinformatics datasets'
                        'with NoSQL databases',
       classifiers=[
              'Development Status :: 3 - Alpha']
)
