import os
from setuptools import setup, find_packages

requirements = open(os.path.join(os.path.dirname(__file__),
                                 'requirements.txt')).readlines()
setup(
       name='nosqlbiosets',
       version='0.0.2',
       install_requires=requirements,
       packages=find_packages(),
       data_files=[('data', ['data/gene2pubtator.sample'])],
       description='Scripts for indexing selected bioinformatics datasets'
                   'with NoSQL databases',
       classifiers=[
              'Intended Audience :: Science/Research',
              'Development Status :: 3 - Alpha'],
       platforms='GNU/Linux, Mac OS X'
)
