import numpy as np
import pandas as pd

from pipedown.visualization import plot_metrics


def test_plot_metrics():

    metrics = pd.DataFrame()
    metrics["model_name"] = ["m1", "m1", "m1", "m2", "m2", "m2"]
    metrics["metric_name"] = ["mean_squared_error"] * 6
    metrics["fold"] = [0, 1, 2, 0, 1, 2]
    metrics["metric_value"] = np.square(np.random.randn(6))

    plot_metrics(metrics)


def test_plot_metrics_multiple_metrics():

    metrics = pd.DataFrame()
    metrics["model_name"] = ["m1"] * 4 + ["m2"] * 4
    metrics["metric_name"] = ["mse", "mse", "mae", "mae"] * 2
    metrics["fold"] = [0, 1] * 4
    metrics["metric_value"] = np.square(np.random.randn(8))

    plot_metrics(metrics)
