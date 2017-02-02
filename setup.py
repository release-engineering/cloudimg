#!/usr/bin/python
from setuptools import setup, find_packages

with open('LICENSE') as fh:
    license = fh.read()

with open('README.md') as fh:
    readme = fh.read()

setup(
    name='cloudimg',
    version='0.1.0',
    author='Alex Misstear',
    author_email='amisstea@redhat.com',
    description=('Services for building and releasing products in cloud '
                 'environments'),
    long_description=readme,
    license=license,
    url='https://gitlab.cee.redhat.com/rad/cloud-image',
    packages=find_packages(exclude=('tests', 'bin', 'docs'))
)
