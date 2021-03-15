import matplotlib.pyplot as plt
import seaborn as sns


def plot_metrics(
    metrics,
    x="model_name",
    y="metric_value",
    metric_col="metric_name",
    metric=None,
    figsize=(6.4, 4.8),
):
    """Plot metrics by model"""

    # If only one metric is present, hue on model
    if metrics[metric_col].nunique() == 1:
        metric = metrics[metric_col].unique()[0]

    # Set up the plot
    sns.set()
    fig, ax = plt.subplots(figsize=figsize)

    # Plot the metrics
    if metric is None:
        sns.boxplot(x=x, y=y, data=metrics, hue=metric_col, ax=ax)
    else:
        sns.boxplot(
            x=x,
            y=y,
            data=metrics[metrics[metric_col]==metric],
            ax=ax
        )
        plt.ylabel(metric)
    plt.xticks(rotation=45, ha='right')
