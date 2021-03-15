import cloudpickle


def save_dag(dag, filename: str):
    """Save a DAG to file"""
    with open(filename, "wb") as fid:
        cloudpickle.dump(dag, fid)


def load_dag(filename: str):
    """Load a DAG from file"""
    with open(filename, "rb") as fid:
        return cloudpickle.load(fid)
