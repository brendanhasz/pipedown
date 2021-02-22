# Pipedown :shushing_face:

Pipedown is a machine learning model pipelining package for Python.  It lets
you define a directed acyclic graph (DAG) of modeling steps, and makes it
easier to run sections of that DAG, perform cross-validation, serialize the
DAG, and visualize the DAG.  Pipedown focuses on:

* Testing: each node is defined as a
# TODO

Pipedown is NOT an ETL / data engineering / task scheduler tool - for that use
something like Airflow, Argo, Dask, Prefect, etc.  You can do some basic and
inefficient data processing with Pipedown, but really it's focused on creating
portable model pipelines.


* Git repository: [http://github.com/brendanhasz/pipedown](http://github.com/brendanhasz/pipedown)
* Documentation:
* Bug reports: [http://github.com/brendanhasz/pipedown/issues](http://github.com/brendanhasz/pipedown/issues)

Still in the super early stages - don't use this yet!

## Requirements

To use the visualization tools, you need to have
[graphviz](https://graphviz.org/) installed.  On Ubuntu, you can install with:

```bash
sudo apt-get install graphviz
```
