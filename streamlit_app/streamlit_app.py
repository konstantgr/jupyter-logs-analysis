import streamlit as st

from pathlib import Path

from sequence import NotebookActionsSequence
from data_tools import get_databases, load_data, get_group
from graph_tools import evolution_to_graphviz, evolution_to_networkx, draw_nx_graph
from st_tools import check_password


def get_pdf_file(graph):

    graph.render('streamlit_app/figures/Digraph.gv.pdf').replace('\\', '/')
    with open("streamlit_app/figures/Digraph.gv.pdf", "rb") as pdf_file:
        pdf_file = pdf_file.read()

    return pdf_file


def app_main_loop():
    dbs = get_databases(Path("data/"))
    dbs_dict = {db.name: db for db in dbs}
    db_option = st.selectbox('Select database', dbs_dict.keys())
    df = load_data(dbs_dict.get(db_option))

    st.write('You selected:', dbs_dict.get(db_option))
    st.dataframe(df.tail(5))

    kernel_option = st.selectbox('Select `kernel_id`', df.kernel_id.unique())
    df_group = get_group(df, "kernel_id", kernel_option)

    try:
        evol = NotebookActionsSequence(df_group)
        snap_num = st.slider('Snapshot number', 0, len(evol.snapshots) - 1, 0)
        snap = evol.snapshots[snap_num]

        g_gv = evolution_to_graphviz(evol, snap_num + 1)
        st.graphviz_chart(g_gv)

        # st.download_button(
        #     label="Download graph as png",
        #     data=csv,
        #     file_name='large_df.csv',
        #     mime='text/csv',
        # )

        # st.download_button(
        #     label="Download graph as PDF",
        #     data=get_pdf_file(g_gv),
        #     file_name=f'graph_snapshot_num_{snap_num}.pdf',
        #     mime='application/octet-stream',
        # )

        st.header("Event")
        st.warning(snap.log)

        st.header("Notebook cells")
        for cell in snap.cells_list:
            st.code(f"# {cell.cell_index}\n\n" + cell.cell_source)

        # g_nx = evolution_to_networkx(evol, snap_num)
        # fig, ax = draw_nx_graph(g_nx)
        # st.pyplot(fig)

    except IndexError:
        st.warning("Something wrong with notebook evolution")


if __name__ == '__main__':
    authorize = check_password()
    authorize = True
    if authorize:
        app_main_loop()
