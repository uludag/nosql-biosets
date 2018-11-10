from setuptools import setup, find_packages

packages = find_packages()

setup(
       name='nosqlbiosets',
       version='0.0.4',
       description='Index/query scripts '
                   'for selected free bioinformatics datasets',
       author='Mahmut Uludag',
       author_email='mahmut.uludag@kaust.edu.sa',
       url='https://bitbucket.org/hspsdb/nosql-biosets',
       license='MIT License',
       install_requires=[
              'argh',
              'elasticsearch',
              'neo4j-driver',
              'networkx',
              'pymongo',
              'six',
              'xmltodict'
       ],
       extras_require={
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
                     'py2cytoscape', 'jinja2'
              ),
              'cobra': (
                     'cobra', 'cobrababel', 'psamm'
              )
       },
       packages=find_packages(),
       scripts=['scripts/nosqlbiosets'],
       classifiers=[
              'Intended Audience :: Science/Research',
              'Development Status :: 3 - Alpha'],
       platforms='GNU/Linux, Mac OS X',
       test_suite='tests'
)
