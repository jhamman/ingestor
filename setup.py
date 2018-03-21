from setuptools import setup

with open('README.md') as f:
    readme = f.read()

INSTALL_REQUIRES = ['xarray', 'pandas', 'joblib']
TESTS_REQUIRE = ['pytest']
EXTRAS_REQUIRE = ['ecmwfapi']


setup(name='ingestor',
      version='0.0',
      description='A library to facilitate the ingestion and normalization of '
                  'climate datasets',
      long_description=readme,
      author='Joe Hamman',
      author_email='jhamman@ucar.edu',
      packages=['ingestor', 'ingestor.tests'],
      install_requires=INSTALL_REQUIRES,
      tests_require=TESTS_REQUIRE,
      extras_require=EXTRAS_REQUIRE,
      classifiers=['License :: OSI Approved :: Apache License',
                   'Programming Language :: Python :: 3'])
