- Node should have __init__(self, name), and subclasses should call super().__init__(name) (instead of __init__ and init...)
- Cache nodes (cache the output of preceeding DAG. Pipeline should stop the dfs of the DAG at cache nodes if their cache is not cleared (is_cached is true).  This will ensure preceeding DAG elements aren't run even with multiple runs, during cross-validation, etc)
- SEPARATE DAG FROM PIPELINE.  Pipeline should be a thing which takes a list of nodes and connects them linearly.  DAG should be what is currently the Pipeline class (where you can arbitrarily define nodes + connections).  or maybe just have DAG have .add_node(node) and/or add_pipeline?  And a clean or clear_nodes or delete_nodes or somesuch (run in between CV folds)?
- Cross validation schemes (cross_validation should have two submodules: splitters (what it is now), and implementations (Sequential (which just does what it does now), ProcessPool (uses pool of processes to do CV in parallel), SparkPool, etc
- Tests for DAG
    * Cache base class in nodes/base (needs to have a is_cached (default=False), clear_cache (deletes the cache and sets is_cached to false), fit (sets the cache and sets is_cached to true), and run (grabs the data from the cache) abstractmethods
    * In nodes/caches, have specific ones like InMemoryCache, CsvCache, FeatherCache, PostgresCache, etc. (CephCache, AzureBlobCache, S3Cache?)
    * In fit(), cache nodes should check to see if their data already exists in the cache (ie run is_cached - so, does the csv file exist; does the feather file exist, etc) and if not, write the input data to that cache.
    * Then, in run, load the data from the cache.
    * Cache nodes should have a clear_cache method which deletes the cache file/database/whatever method is being used.
    * Also, DAG should have a clear_caches method which runs clear_cache on every Cache node in the pipeline


Maybe:

class DAG:

    def nodes(self):
        return {
            'node_name': NodeObj(),
            'node_name2': NodeObj2(),
            # ...
        }

    def edges(self):
        return {
            'child_node_name': 'parent_node_name',
            'child_node_name2': ['parent_node_name1', 'parent_node_name2'],
            'child_node_name3': {'kwarg1': 'parent3', 'kwarg2': 'parent4'},
            'primary': {'test': 'input', 'train': 'data_loader'},
        }

    get_node(node_name)
    delete_nodes()  # deletes all the nodes

    optionally users can implement __init__ and then use attrs in nodes/edges

