# cloudimg
A library for uploading and publishing disk images on various clouds

[![Build Status](https://travis-ci.org/release-engineering/cloudimg.svg?branch=master)](https://travis-ci.org/release-engineering/cloudimg)
[![Coverage Status](https://coveralls.io/repos/github/release-engineering/cloudimg/badge.svg?branch=master)](https://coveralls.io/github/release-engineering/cloudimg?branch=master)

## Installation

```python setup.py install```

## Development

It's best to develop with python 3.6 since that is the minimum supported
version and tends to have the most restrictive features.

```
# Setup a virtual environment
virtualenv -p python3.6 venv
source venv/bin/activate

# Install the package for development
python setup.py develop
```

Additionally, run this for test dependencies:

`pip install -r requirements-test.txt`

## Running lint checks

Run either:

`tox -e lint`

OR:

`flake8 cloudimg/ tests/`

## Running unit tests

Unit tests can be executed manually or via `tox` to cover all the supported
python interpreters.

### Manual execution

`py.test`

### Execution with tox

Prior to using tox, ensure you have installed all the supported versions of
python on your system (check tox.ini).

After that, simply run:

`tox`

### License

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
