import sys

from setuptools import setup, find_packages

py35 = (sys.version_info[0] == 3 and sys.version_info[1] >= 5)

setup(
       name='nosqlbiosets',
       version='0.0.5',
       description='Index/query scripts '
                   'for selected free bioinformatics datasets',
       author='Mahmut Uludag',
       author_email='mahmut.uludag@kaust.edu.sa',
       url='https://bitbucket.org/hspsdb/nosql-biosets',
       license='MIT License',
       install_requires=[
           'argh',
           'elasticsearch',
           'networkx' if py35 else 'networkx==2.2',
           'pymongo',
           'six',
           'xmltodict'
       ],
       extras_require={
              'gffutils': (
                     'gffutils'
              ),
              'neo4j': (
                     'neo4j-driver'
              ),
              'pivottablejs': (
                     'pivottablejs',
              ),
              'sqlalchemy': (
                     'SQLAlchemy'
              ),
              'pandas': (
                     'pandas'
              ),
              'py2cytoscape': (
                     'py2cytoscape'
              ),
              'cobra': (
                     'cobra', 'cobrababel', 'psamm'
              )
       },
       packages=find_packages(),
       scripts=['scripts/nosqlbiosets'],
       keywords=['bioinformatics'],
       classifiers=[
              'Intended Audience :: Science/Research',
              'Development Status :: 3 - Alpha'],
       platforms='GNU/Linux, Mac OS X',
       test_suite='tests'
)
