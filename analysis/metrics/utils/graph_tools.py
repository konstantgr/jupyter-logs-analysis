from typing import Tuple, Any, Optional

import graphviz
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import pydotplus

from analysis.dataset.june_dataset import NotebookState


def evolution_to_execution_path(
        evolution: list[NotebookState],
        max_snap_num: int,
        min_snap_num: int = 1,
        hash_string_num: int = 8,
) -> tuple[list[tuple[Any, Any]], list[Any]]:
    edges, prev_cell_index = [], "0"

    deleted_cells = [
        x.log.get('cell_index')
        for x in evolution[min_snap_num:max_snap_num]
        if x.log.get('event') == "delete"
    ]

    executions = filter(
        lambda x: (x.log.get('event') == "execute") and (x.log.get('cell_index') not in []),
        evolution[min_snap_num:max_snap_num]
    )

    for snap in executions:
        cell_index = snap.log.get('cell_index')

        cur_cell_index = cell_index[:hash_string_num]
        edges.append((prev_cell_index, cur_cell_index))
        prev_cell_index = cur_cell_index

    return edges, deleted_cells


def evolution_to_graphviz(
        evolution: list[NotebookState],
        max_snap_num: int,
        min_snap_num: int = 1,
        hash_string_num: int = 8,
) -> graphviz.Digraph:
    graph = graphviz.Digraph()

    graph.attr(rankdir='LR', size='10,10')
    graph.attr('edge', weight='0', arrowhead='none')

    prev = '0'
    for i, (index, _) in enumerate(evolution[max_snap_num - 1].index_order):
        curr = index[:hash_string_num]
        graph.edge(prev, curr, weight='0')
        prev = curr

    # graph.attr(rankdir='LR')
    #
    # for i, (index, _) in enumerate(evolution.snapshots[max_snap_num].index_order):
    #     graph.node(index[:hash_string_num], weight='1')
    #
    edges, deleted_cells = evolution_to_execution_path(
        evolution, max_snap_num, min_snap_num, hash_string_num
    )

    graph.attr('node', weight='0')

    for cell_index in deleted_cells:
        graph.node(
            cell_index[:hash_string_num],
            fillcolor='lightgrey', style='filled'
        )

    graph.attr('edge', weight='0', arrowhead='vee')

    for i, edge in enumerate(edges):
        graph.edge(*edge, label=f"{i}")

    return graph


def dataframe_to_graphviz(
        df: pd.DataFrame,
        graph: Optional[nx.Graph | nx.DiGraph] = None,
        min_state_num: Optional[int] = None, max_state_num: Optional[int] = None,
        hash_string_num: int = 8,
) -> graphviz.Digraph:
    edges, prev_cell_index = [], "0"
    graph = graphviz.Digraph() if graph is None else graph

    executions = df.iloc[min_state_num:max_state_num][df.event == "execute"]
    for i, (_, log_row) in enumerate(executions.iterrows()):
        cell_index = log_row.cell_index

        cur_cell_index = cell_index[:hash_string_num]
        graph.edge(prev_cell_index, cur_cell_index, label=f"{i}")
        prev_cell_index = cur_cell_index
    return graph


def graphviz2networkx(g):
    dotplus = pydotplus.graph_from_dot_data(g.source)
    nx_graph = nx.nx_pydot.from_pydot(dotplus)
    return nx_graph


def evolution_to_networkx(
        evolution: list[NotebookState],
        max_snap_num: int,
        min_snap_num: int = 1,
        hash_string_num: int = 8,
) -> nx.MultiDiGraph:
    graph = nx.MultiDiGraph()
    edges = evolution_to_execution_path(
        evolution, max_snap_num, min_snap_num, hash_string_num
    )

    # edges = [(*e, {'N': i}) for i, e in enumerate(edges)]
    graph.add_edges_from(edges[0])
    return graph


def draw_nx_graph(
        graph: nx.MultiDiGraph,
        fig_ax: [Tuple, None] = None
) -> Tuple:
    if fig_ax is None:
        fig, ax = plt.subplots(figsize=(5, 5))
    else:
        fig, ax = fig_ax

    pos = nx.circular_layout(graph)
    nx.draw_networkx_nodes(
        graph, pos, ax=ax, node_color='k',
        node_size=100, alpha=1, label=True
    )

    # nx.draw_networkx_labels(G, pos, {i: i for i in set(labels)})
    for e in graph.edges:
        ax.annotate(
            "", xy=pos[e[0]], xycoords='data',
            xytext=pos[e[1]], textcoords='data',
            arrowprops=dict(
                arrowstyle="<-", color="0.5",
                shrinkA=5, shrinkB=5,
                patchA=None, patchB=None,
                connectionstyle="arc3,rad=rrr".replace('rrr', str(0.3 * e[2])),
            ),
        )
    return fig, ax


if __name__ == '__main__':
    print("dummy")
