# flake8: noqa

try:
    from importlib_metadata import version
except ImportError:
    from importlib.metadata import version  # Python 3.8+

try:
    __version__ = version("necstdb")
except:
    __version__ = None

from .necstdb import opendb
from .necstdb import relog
from . import utils
import xarray
