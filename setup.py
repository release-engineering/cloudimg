from setuptools import setup, find_packages

setup(
    name='cloudimg',
    version='0.1.0',
    author='Alex Misstear',
    author_email='amisstea@redhat.com',
    description=('Services for building and releasing products in cloud '
                 'environments'),
    license='GPLv3',
    url='https://gitlab.cee.redhat.com/rad/cloud-image',
    packages=find_packages(exclude=('tests', 'bin', 'docs')),
    install_requires=[
        'apache-libcloud>=1.5.0',
        'requests',
    ]
)
