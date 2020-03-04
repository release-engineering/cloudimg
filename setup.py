from setuptools import setup, find_packages

setup(
    name='cloudimg',
    version='0.2.7',
    author='Alex Misstear',
    author_email='amisstea@redhat.com',
    description=('Services for building and releasing products in cloud '
                 'environments'),
    license='GPLv3',
    url='https://github.com/release-engineering/cloudimg',
    packages=find_packages(),
    install_requires=[
        'boto3',
        'requests',
    ]
)
