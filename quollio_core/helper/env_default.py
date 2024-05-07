"""
https://gist.github.com/orls/51525c86ee77a56ad396
Provides a utility to inject environment variables into argparse definitions.
Currently requires explicit naming of env vars to check for
"""

import argparse
import os


# Courtesy of http://stackoverflow.com/a/10551190 with env-var retrieval fixed
class EnvDefault(argparse.Action):
    """An argparse action class that auto-sets missing default values from env
    vars. Defaults to requiring the argument."""

    def __init__(self, envvar, required=True, default=None, **kwargs):
        # override values if envvar exists
        if envvar in os.environ:
            if kwargs.get("nargs", None) is None:
                default = os.environ[envvar]
            else:
                default = os.environ[envvar].split(" ")
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


# functional sugar for the above
def env_default(envvar):
    def wrapper(**kwargs):
        return EnvDefault(envvar, **kwargs)

    return wrapper
