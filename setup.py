from setuptools import setup, find_packages

setup(
    name='cloudimg',
    version='0.1.1',
    author='Alex Misstear',
    author_email='amisstea@redhat.com',
    description=('Services for building and releasing products in cloud '
                 'environments'),
    license='GPLv3',
    url='https://gitlab.cee.redhat.com/rad/cloud-image',
    packages=find_packages(),
    install_requires=[
        'apache-libcloud>=2.0.0',
        'requests',
    ]
)
