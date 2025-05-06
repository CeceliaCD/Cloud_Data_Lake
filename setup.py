from setuptools import setup, find_packages

setup(
    name='pokemon_datalake_and_anltx',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        "pandas==1.3.3"
    ],
    include_package_data=True,
    description='The .whl package of pokemon datalake and analytics project.',
)