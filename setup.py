from setuptools import setup, find_packages

setup(
    name='pokemon_datalake_and_anltx',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        "numpy<1.25.0,>=1.21.0",
        "pandas<2.0.0,>=1.2.0",
        "pyarrow<7.1.0,>=2.0.0",
        "charset-normalizer<3.0,>=2.0",
        "requests<2.27.2,>=2.23.0",
        "urllib3<1.27,>=1.25.4",
        "pytz<2022.2,>=2020.1",
        "botocore==1.24.21",
        "awswrangler==2.15.1",
        "scipy==1.8.0",
        "redshift-connector==2.0.907",
        "charset-normalizer<3.0,>=2.0",
        "avro-python3"
    ],
    include_package_data=True,
    description='The .whl package of pokemon datalake and analytics project.',
)