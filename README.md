# Pipedown :shushing_face:

A data science pipelining framework for Python.

Still in the super early stages - don't use this yet!

Roadmap:

* Actually getting the main package working.
* Ensembling? Could just have a node w/ multiple inputs from multiple models; combines predictions via stacking or averaging or another model or whatever you like.
* Design for integration with experiment tracking packages (MLFlow, Optuna, HyperparameterHunter, etc), esp. in terms of hyperparam optimization + feature selection:
* Hyperparameter optimization?  Node objs could just have a hyperparameters method which defines default vals and/or range, then also has a get_hyperparameter('name') method used in fit and run.  And Pipeline could have a optimize_hyperparameters fn which optimizes the hyperparams of all its nodes jointly.  Should think about integration with other hyperparam opt packages though (hyperopt, scikit-optimize, optuna, ray.tune)
* Feature selection (again, obvi can just have a single node which does, say PCA, or includes a model, but would be nice to have it take the output from a model for e.g. permutation importance-based selection or sequential feature selection)
* Reports? - generate plots or somesuch? I mean of course can just have a node which saves files etc
* Feature importance?
* Partial dependence + SHAP values?
* How to work probabilistic predictions into the framework?  Ie what about Bayesian models?  The current Model specification (returning just y_pred) won't work.


Notes:

- When instantiating a node with an `__init__`, make sure to call `super().__init__()`!
