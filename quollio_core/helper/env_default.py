"""
https://gist.github.com/orls/51525c86ee77a56ad396
Provides a utility to inject environment variables into argparse definitions.
Currently requires explicit naming of env vars to check for
"""

import argparse
import os
from distutils.util import strtobool
from typing import Union


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


class EnvStoreTrue(argparse._StoreTrueAction):
    """An argparse action class that auto-sets missing default values from env vars for store_true."""

    def __init__(self, envvar, required=True, default=None, **kwargs):
        # Only pass the arguments that argparse._StoreTrueAction expects
        action_kwargs = {key: value for key, value in kwargs.items() if key in ("option_strings", "dest")}
        if envvar in os.environ:
            default = _convert_value_to_bool(os.environ[envvar])
        if required and default:
            required = False
        super(EnvStoreTrue, self).__init__(default=default, required=required, **action_kwargs)


# functional sugar for the above
def env_default(envvar, store_true=False):
    def wrapper(**kwargs):
        if store_true:
            return EnvStoreTrue(envvar, **kwargs)
        return EnvDefault(envvar, **kwargs)

    return wrapper


def _convert_value_to_bool(v: Union[str, bool]) -> bool:
    if isinstance(v, str):
        return bool(strtobool(v))
    return v
