# cloudimg
A library for uploading and publishing disk images on various clouds

## Installation

`python setup.py install`

## Development

`python setup.py develop`

Additionally, run this for test dependencies:

`pip install -r requirements-test.txt`

### Git hooks

```
chmod +x setup-git-hooks.sh
./setup-git-hooks.sh
```

## Running unit tests

Unit tests can be executed manually or via `tox` to cover all the supported
python interpreters.

### Manual execution

`nosetests tests/`

### Execution with tox

Prior to using tox, ensure you have installed all the supported versions of
python on your system:
	2.6, 2.7, 3.3, 3.4 and 3.5

After that, simply run:

`tox`
