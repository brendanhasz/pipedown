- Tests for DAG
- ProcessPool cv_implementation
- Implement commonly used nodes
- Visualization improvements:
    * Initally show the DAG filling the dag-viewer div, centered vertically and horizontally
    * Show inputs/outputs in info div (after node has been fit)
    * Show code in info div (w link to github? maybe just have an optional class attribute w/ link to gh)


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
