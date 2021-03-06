"""
The pipedown.cross_validation.splitters module contains classes for different
dataset-splitting schemes for cross validation.

* :class:`.CrossValidationSplitter` - abstract base class for all splitters
* :class:`.OutOfTimeSplitter` - out-of-time cross-val for timeseries
* :class:`.RandomSplitter` - random split cross-validation
* :class:`.StratifiedSplitter` - stratified cross-val by class
* :class:`.TimeBinSplitter` - split using specific time bins
"""

__all__ = [
    "CrossValidationSplitter",
    "OutOfTimeSplitter",
    "RandomSplitter",
    "StratifiedSplitter",
    "TimeBinSplitter",
]

from .cross_validation_splitter import CrossValidationSplitter
from .out_of_time_splitter import OutOfTimeSplitter
from .random_splitter import RandomSplitter
from .stratified_splitter import StratifiedSplitter
from .time_bin_splitter import TimeBinSplitter
