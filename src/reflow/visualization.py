from typing import Any, Callable, Tuple

import pandas as pd

from .recipe import Key
from .utils.execution import ensure_tuple

StatsMapper = Callable[..., dict[Tuple, Any]]


def results_to_dataframe(
    results: list[tuple[Key, Any]],
    result_to_stats: dict[str, StatsMapper] | StatsMapper | None = None,
    filter_columns: bool = True,
    set_index: bool = True,
    set_index_drop_output_descriptor: bool = True,
):
    """
    Converts results to a DataFrame.

    TODO: allow to get multiple results as input for a StatsMapper

    Parameters
    ----------
    results : list[tuple[Key, Any]]
        List of results.
    result_to_stats : dict[str, StatsMapper] | StatsMapper | None, optional
        Function to convert results to stats, by default None
    filter_columns : bool, optional
        Filter columns with only one unique value, by default True
    set_index : bool, optional
        Set index to options, by default True
    """
    stats_rows = []
    for options, result in results:
        stats_dict = {}
        for step, v in result.items():
            if result_to_stats is None:
                stats_dict[("results", step)] = v
            elif callable(result_to_stats):
                stats = result_to_stats(options, v)
                for stat_key, stat_value in stats.items():
                    key = ("results", step, *ensure_tuple(stat_key))
                    stats_dict[key] = stat_value
            else:
                if step not in result_to_stats:
                    stats_dict[("results", step)] = v
                else:
                    stats = result_to_stats[step](options, v)
                    for stat_key, stat_value in stats.items():
                        key = ("results", step, *ensure_tuple(stat_key))
                        stats_dict[key] = stat_value

        stats_rows.append(stats_dict)

    stats_df = pd.DataFrame.from_records(stats_rows)
    columns = stats_df.columns
    max_length = max(len(c) for c in columns)
    columns = [(*c, *([""] * (max_length - len(c)))) for c in columns]
    stats_df.columns = pd.MultiIndex.from_tuples(columns)

    options_dicts = [options for options, _ in results]
    df_options = pd.DataFrame(options_dicts)
    df_options.columns = pd.MultiIndex.from_tuples(
        [
            ("options", step, *([""] * (stats_df.columns.nlevels - 2)))
            for step in df_options.columns
        ]
    )

    df = pd.concat([df_options, stats_df], axis=1)

    if filter_columns:
        drop_idx = df.columns[df.nunique() <= 1]
        df.drop(columns=drop_idx, inplace=True)

    if set_index:
        df.set_index(df[["options"]].columns.tolist(), inplace=True)
        # TODO: I would like a hierarchical multiindex :(
        df.index.names = [c[1] for c in df.index.names]
        df.columns = df.columns.droplevel(0)

    return df


# def draw(
#         recipe: Recipe,
#         include: PathPatterns = None,
#         exclude: PathPatterns = None,
#         last_step:str=None,
#         tree:bool=False,
#         label_format=None,
#         label_rotation=35,
#         draw_kwargs=None,
#         draw_labels_kwargs=None):

#     # TODO: improve

#     dag = recipe.dag(
#         include=include,
#         exclude=exclude,
#         last_step=last_step)
#     if label_format is None:
#         label_format = lambda x: x
#     if draw_kwargs is None:
#         draw_kwargs = {}
#     if draw_labels_kwargs is None:
#         draw_labels_kwargs = {}

#     # plot dag
#     if not tree:
#         pos = nx.get_node_attributes(dag, 'pos')
#         nx.draw(
#             dag,
#             pos,
#             **draw_kwargs)

#         labels = dag.nodes()
#         text = nx.draw_networkx_labels(
#             tree,
#             pos,
#             labels={n:label_format(n) for n in labels},
#             **{**{"font_size": 8}, **draw_labels_kwargs})
#         for _, t in text.items():
#             t.set_rotation(label_rotation)

#     else:
#         # shortest paths
#         tree = nx.dag_to_branching(dag)

#         # visualize tree
#         pos = nx.nx_agraph.graphviz_layout(tree, prog="dot", )
#         nx.draw(
#             tree, pos,
#             **draw_kwargs)

#         labels = nx.get_node_attributes(tree, 'source')
#         text = nx.draw_networkx_labels(
#             tree,
#             pos,
#             labels={n:label_format(l) for n,l in labels.items()},
#             **{**{"font_size": 8}, **draw_labels_kwargs})
#         for _, t in text.items():
#             t.set_rotation(label_rotation)
