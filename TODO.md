- Don't have nodes store _parents and _children, have the DAG set those in materialize_dag.  Also means no need for super().__init__() everywhere
- Tests for DAG
- Cache nodes (cache the output of preceeding DAG. Pipeline should stop the dfs of the DAG at cache nodes if their cache is not cleared (is_cached is true).  This will ensure preceeding DAG elements aren't run even with multiple runs, during cross-validation, etc)
    * Cache base class in nodes/base (needs to have a is_cached (default=False), clear_cache (deletes the cache and sets is_cached to false), fit (sets the cache and sets is_cached to true), and run (grabs the data from the cache) abstractmethods
    * In nodes/caches, have specific ones like InMemoryCache, CsvCache, FeatherCache, PostgresCache, etc. (CephCache, AzureBlobCache, S3Cache?)
    * In fit(), cache nodes should check to see if their data already exists in the cache (ie run is_cached - so, does the csv file exist; does the feather file exist, etc) and if not, write the input data to that cache.
    * Then, in run, load the data from the cache.
    * Cache nodes should have a clear_cache method which deletes the cache file/database/whatever method is being used.
    * Also, DAG should have a clear_caches method which runs clear_cache on every Cache node in the pipeline
- ProcessPool cv_implementation
- Visualization (exporting html/javascript of the viewer)
    * https://www.petercollingridge.co.uk/tutorials/svg/interactive/mouseover-effects/



So DAG implementation should go like this:

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


Longer term TODO:

* Actually getting the main package working (above)
* Ensembling? Could just have a node w/ multiple inputs from multiple models; combines predictions via stacking or averaging or another model or whatever you like.
* Design for integration with experiment tracking packages (MLFlow, Optuna, HyperparameterHunter, etc), esp. in terms of hyperparam optimization + feature selection:
* Hyperparameter optimization?  Node objs could just have a hyperparameters method which defines default vals and/or range, then also has a get_hyperparameter('name') method used in fit and run.  And Pipeline could have a optimize_hyperparameters fn which optimizes the hyperparams of all its nodes jointly.  Should think about integration with other hyperparam opt packages though (hyperopt, scikit-optimize, optuna, ray.tune)
* Feature selection (again, obvi can just have a single node which does, say PCA, or includes a model, but would be nice to have it take the output from a model for e.g. permutation importance-based selection or sequential feature selection)
* Reports? - generate plots or somesuch? I mean of course can just have a node which saves files etc
* Feature importance?
* Partial dependence + SHAP values?
* How to work probabilistic predictions into the framework?  Ie what about Bayesian models?  The current Model specification (returning just y_pred) won't work.
