import pandas as pd

from pipedown.nodes.base.loader import Loader


class SqlAlchemyLoader(Loader):
    """Loads data via a SQL query using SQLAlchemy

    .. admonition::

        This Loader requires the sqlalchemy pakage is installed, and also the
        jinja2 package if you use `query_kwargs`.

    Parameters
    ----------
    engine : sqlalchemy engine
        SqlAlchemy engine for connecting to the database
    query_filename : str
        Filename of a file containing the SQL query.  The file can use Jinja
        templating, and `query_args` will be used as the template parameters.
    query_kwargs : dict
        Dictionary of arguments to be used to template the query
    """

    def __init__(self, engine, query_filename, query_kwargs={}):
        self.engine = engine
        self.query = self.format_query(query_filename, query_kwargs)

    def format_query(self, query_filename, query_kwargs):
        with open(query_filename, "r") as fid:
            if query_args:
                from jinja2 import Template

                return Template(fid.read()).render(**query_kwargs)
            else:
                return fid.read()

    def run(self):
        return pd.read_sql(self.query, self.engine)
