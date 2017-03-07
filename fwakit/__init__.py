from __future__ import absolute_import
import os

import yaml

from fwakit.fwa import FWA


CONFIG = os.path.join(os.path.dirname(__file__), 'config.yml')

__version__ = "0.0.1"

with open(CONFIG) as config_file:
    config = yaml.load(config_file)
