from setuptools import setup

setup(
    name='sgcn',
    version='0.0.1',
    description='Processing codes for building the SGCN distribution',
    url='https://github.com/blohre-usgs/pipeline_SGCN',
    author='Sky Bristol, Ben Lohre',
    author_email='bcb@usgs.gov',
    license='unlicense',
    packages=['sgcn'],
    install_requires=['requests', 'ftfy', 'xmltodict', 'bis'],
    zip_safe=False
)