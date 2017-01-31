from ConfigParser import ConfigParser
import os

CONFIG_FILENAME = 'cloudimg.conf'
CONFIG_ENV_VAR = 'CLOUDIMG_CONF'


class Config(object):
    """
    A wrapper for configs. Allows for lazy access since the config will not be
    read until the first attribute is accessed.

    An attribute from the config can be accessed by concatenating the section
    with the name separated by an '_'. E.g. 'config.AWS_ACCESS_ID' will search
    the 'aws' section for attribute called 'access_id'.

    Args:
        filename (str): The name of the config file
        env_var (str, optional): An environment variable which might contain a
            path to the config file.
    """

    def __init__(self, filename, env_var=None):
        self.filename = filename
        self.env_var = env_var
        self.config = None

    def load(self):
        """
        Loads the config by searching for the file in the following order:

        1) The current working directory
        2) The user's home directory
        3) /etc/cloudimg/
        4) The value of the environment variable passed

        Returns:
            A ConfigParser object
        """

        # Ordered search paths for config
        config_locations = [
            os.path.join(os.curdir, self.filename),
            os.path.join(os.path.expanduser('~'), self.filename),
            os.path.join('/etc/cloudimg/', self.filename),
        ]

        # Prioritize environment var search path if it is defined
        env_var_value = os.environ.get(self.env_var)
        if env_var_value:
            config_locations.insert(0, env_var_value)

        # Return the first one found
        for location in config_locations:
            path = os.path.abspath(location)
            if not os.path.exists(path):
                continue

            config = ConfigParser()
            config.read(path)
            return config

        raise RuntimeError('{0} not found'.format(self.filename))

    def __getattr__(self, name):
        if not self.config:
            self.config = self.load()
        section, attr = name.lower().split('_', 1)
        return self.config.get(section, attr)

config = Config(CONFIG_FILENAME, env_var=CONFIG_ENV_VAR)
