import pandas as pd

from pipedown.visualization.node_drawers import square_box_json_icon

from .node import Node


class Input(Node):
    """Represents data input at test/inference time.

    Takes test data in one of the following forms:

    * Pandas DataFrame representing one or more datapoints
    * Pandas Series representing a single datapoint
    * List of dicts represeting one or more datapoints ("record" format)
    * Dict of lists representing one or more datapoints ("column" format)
    * Dict representing a single datapoint
    """

    draw = square_box_json_icon

    def run(self, data, format='auto'):
        """Convert data to DataFrame"""

        # Check format is valid
        if format not in [
            "dataframe",
            "series",
            "rows",
            "columns",
            "single",
            "auto",
        ]:
            raise ValueError("Invalid format")

        # Convert the data
        if format == "dataframe" or (
            format == "auto" and isinstance(data, pd.DataFrame)
        ):
            return data
        elif format == "series" or (
            format == "auto" and isinstance(data, pd.Series)
        ):
            return pd.DataFrame.from_records([data.to_dict()])
        elif format == "rows" or (
            format == "auto"
            and isinstance(data, list)
            and all(isinstance(e, dict) for e in data)
        ):
            return pd.DataFrame.from_records(data)
        elif format == "columns" or (
            format == "auto"
            and isinstance(data, dict)
            and all(isinstance(v, list) for _, v in data.items())
        ):
            return pd.DataFrame.from_dict(data)
        else:  # assume just one record
            return pd.DataFrame.from_records([data])
