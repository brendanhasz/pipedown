"""
The cross_validation module contains classes for different cross validation
schemes.

* :class:`.CrossValidator` - abstract base class for all cross-validators
* :class:`.OutOfTimeCrossValidator` - out-of-time cross-val for timeseries
* :class:`.RandomCrossValidator` - random split cross-validation
* :class:`.StratifiedCrossValidator` - stratified cross-val by class
"""

__all__ = [
    "CrossValidator",
    "OutOfTimeCrossValidator",
    "RandomCrossValidator",
    "StratifiedCrossValidator",
]

from .cross_validator import CrossValidator
from .out_of_time import OutOfTimeCrossValidator
from .random import RandomCrossValidator
from .stratified import StratifiedCrossValidator
