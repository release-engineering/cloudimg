from setuptools import setup, find_packages

setup(
    name='cloudimg',
    version='1.14.0',
    author='Alex Misstear',
    author_email='amisstea@redhat.com',
    description=('Services for building and releasing products in cloud '
                 'environments'),
    license='GPLv3',
    url='https://github.com/release-engineering/cloudimg',
    packages=find_packages(),
    python_requires=">=3.6",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8"
    ],
    install_requires=[
        'azure-storage-blob',
        'azure-mgmt-storage',
        'attrs',
        'boto3',
        'monotonic; python_version < \'3.3\'',
        'requests',
        "tenacity",
    ]
)
