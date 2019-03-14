from setuptools import setup, find_packages
from os import path

# root directory
here = path.abspath(path.dirname(__file__))

with open(path.join(here, '../README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().splitlines()

setup(
    name='mapqueue',
    version='0.0.1',
    description='Standard API to store and read data using Map and Queue',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ajmarcus/mapqueue',
    author='Ariel Marcus',
    author_email='hi+mapqueue@arielmarcus.com',
    packages=['mapqueue'],
    python_requires=">=3.4",
    install_requires=requirements,
    extras_require={},
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database :: Front-Ends',
        'Operating System :: OS Independent',
    ],
)