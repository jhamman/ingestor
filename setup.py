from distutils.core import setup

with open('README.md') as f:
    readme = f.read()

setup(name='ingestor',
      version='0.0',
      description='A library to facilitate the ingestion and normalization of '
                  'climate datasets',
      long_description=readme,
      author='Joe Hamman',
      author_email='jhamman@ucar.edu',
      packages=['ingestor', 'ingestor.tests'],
      classifiers=['License :: OSI Approved :: Apache License',
                   'Programming Language :: Python :: 3'])
