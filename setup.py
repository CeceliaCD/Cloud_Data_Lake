from setuptools import setup, find_packages

setup(
    name='pokemonetl',
    version='0.1.0',
    packages=find_packages(include=['pokemonetl', 'pokemonetl.*', 'config', 'config.*', 'sample_data', 'sample_data.*']),
    install_requires=[
        #'pandas<2.0.0',
        #'botocore<=1.25.5',
        #'s3transfer<0.6.0',
        #'aiobotocore>=2.2.0',
        #'scipy==1.8.0',
        #'numpy==1.21.0',
        #'redshift-connector==2.0.907',
        #'pytz==2020.1',
        #'awswrangler==2.15.1',
        #'awscli==1.23.5'
    ],
    include_package_data=True,
    package_data={
        'pokemonetl.queries': ['*.sql']
    },
    description='The etl modules and sql queries for pokemon datalake and analytics project.',
)