from setuptools import setup, find_packages

setup(
    name='src_etl',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'boto3'
    ],
    include_package_data=True,
    description='The etl modules and sql queries for pokemon datalake and analytics project.',
)