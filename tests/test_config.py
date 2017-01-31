from ConfigParser import NoOptionError, NoSectionError
import os
import unittest

from mock import patch

from cloudimg import config, CONFIG_FILENAME, CONFIG_ENV_VAR

HERE = os.path.abspath(os.path.dirname(__file__))


class TestConfig(unittest.TestCase):

    @patch('cloudimg.ConfigParser.read')
    @patch('cloudimg.os.path.exists')
    def check_load(self, expected, mock_exists, mock_read):
        def side_effect(path):
            return expected == path
        mock_exists.side_effect = side_effect
        config.load()
        mock_read.assert_called_once_with(expected)

    def test_load_from_current_dir(self):
        """
        Test the config can be loaded from the current working directory.
        """
        expected = os.path.join(os.getcwd(), CONFIG_FILENAME)
        self.check_load(expected)

    def test_load_from_user_dir(self):
        """
        Test the config can be loaded from the user's home directory.
        """
        expected = os.path.join(os.path.expanduser('~'), CONFIG_FILENAME)
        self.check_load(expected)

    def test_load_from_etc(self):
        """
        Test the config can be loaded from /etc/cloudimg.
        """
        expected = os.path.join('/etc/cloudimg', CONFIG_FILENAME)
        self.check_load(expected)

    def test_load_from_env_var(self):
        """
        Test the config can be loaded from an environment variable.
        """
        expected = os.environ[CONFIG_ENV_VAR] = '/some/config/path'
        self.check_load(expected)

    @patch('cloudimg.os.path.exists')
    def test_does_not_exist(self, mock_exists):
        """
        Test that an exception is raised when the config doesn't exist.
        """
        mock_exists.return_value = False
        del os.environ[CONFIG_ENV_VAR]
        with self.assertRaises(RuntimeError):
            config.load()

    def test_attr_found(self):
        """
        Test that an attribute can be retrieved from the config.
        """
        os.environ[CONFIG_ENV_VAR] = os.path.join(HERE, 'sample.conf')
        config.load()
        self.assertEqual(config.AWS_ACCESS_ID, 'fakeaccessid')

    def test_attr_not_found(self):
        """
        Test that a non-existent config attribute raises an exception.
        """
        os.environ[CONFIG_ENV_VAR] = os.path.join(HERE, 'sample.conf')
        config.load()
        with self.assertRaises(NoOptionError):
            config.AWS_INVALID_ATTR

    def test_section_not_found(self):
        """
        Test that a non-existent config section raises an exception.
        """
        os.environ[CONFIG_ENV_VAR] = os.path.join(HERE, 'sample.conf')
        config.load()
        with self.assertRaises(NoSectionError):
            config.INVALID_SECTION

if __name__ == '__main__':
    unittest.main()
