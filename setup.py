from setuptools import setup, find_packages

setup(
    name='cloudimg',
    version='1.3.0',
    author='Alex Misstear',
    author_email='amisstea@redhat.com',
    description=('Services for building and releasing products in cloud '
                 'environments'),
    license='GPLv3',
    url='https://github.com/release-engineering/cloudimg',
    packages=find_packages(),
    install_requires=[
        'azure-storage-blob',
        'azure-mgmt-storage',
        'attrs',
        'boto3',
        'monotonic; python_version < \'3.3\'',
        'requests',
    ]
)
