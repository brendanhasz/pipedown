# :hankey: Drainpype :hankey:

> A data science pipelining framework for Python to help you guide the flow of sh*tty data


Roadmap:

* Actually getting the main package working.
* Ensembling? Could just have a node w/ multiple inputs from multiple models; combines predictions via stacking or averaging or another model or whatever you like.
* Hyperparameter optimization?  Node objs could just have a hyperparameters method which defines default vals and/or range, then also has a get_hyperparameter('name') method used in fit and run.  And Pipeline could have a optimize_hyperparameters fn which optimizes the hyperparams of all its nodes jointly.  Should think about integration with other hyperparam opt packages though (hyperopt, scikit-optimize, optuna, ray.tune)
* Design for integration with experiment tracking packages (MLFlow, Optuna, HyperparameterHunter, etc)
* Reports? - generate plots or somesuch? I mean of course can just have a node which saves files etc
* Feature importance?
* Partial dependence + SHAP values?
* How to work probabilistic predictions into the framework?  Ie what about Bayesian models?  The current Model specification (returning just y_pred) won't work.

