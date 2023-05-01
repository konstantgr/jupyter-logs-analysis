import graphviz
import networkx as nx
import matplotlib.pyplot as plt

from typing import List, Tuple
from sequence import NotebookActionsSequence


def evolution_to_execution_path(
        evolution: NotebookActionsSequence,
        max_snap_num: int,
        min_snap_num: int = 1,
        hash_string_num: int = 8,
) -> List[Tuple[str, str]]:
    edges, prev_cell_index = [], "0"
    executions = filter(
        lambda x: x.log.get('event') == "execute",
        evolution.snapshots[min_snap_num:max_snap_num]
    )

    for snap in executions:
        cell_num = snap.log.get('cell_num')
        cell_idx = snap.nums_list.index(cell_num)

        cur_cell_index = snap.cells_list[cell_idx].cell_index[:hash_string_num]
        edges.append((prev_cell_index, cur_cell_index))
        prev_cell_index = cur_cell_index

    return edges


def evolution_to_graphviz(
        evolution: NotebookActionsSequence,
        max_snap_num: int,
        min_snap_num: int = 1,
        hash_string_num: int = 8,
) -> graphviz.Digraph:
    graph = graphviz.Digraph()
    graph.attr(rankdir='LR', size='8,8')

    edges = evolution_to_execution_path(
        evolution, max_snap_num, min_snap_num, hash_string_num
    )

    for i, edge in enumerate(edges):
        graph.edge(*edge, label=f"{i}")

    deleted_cells = [
        x.log.get('cell_index')
        for x in evolution.snapshots[min_snap_num:max_snap_num]
        if x.log.get('event') == "delete"
    ]

    for cell_index in deleted_cells:
        graph.node(
            cell_index[:hash_string_num],
            fillcolor='lightgrey', style='filled'
        )

    return graph


def evolution_to_networkx(
        evolution: NotebookActionsSequence,
        max_snap_num: int,
        min_snap_num: int = 1,
        hash_string_num: int = 8,
) -> nx.MultiDiGraph:
    graph = nx.MultiDiGraph()
    edges = evolution_to_execution_path(
        evolution, max_snap_num, min_snap_num, hash_string_num
    )

    edges = [(*e, {'N': i}) for i, e in enumerate(edges)]
    graph.add_edges_from(edges)
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
