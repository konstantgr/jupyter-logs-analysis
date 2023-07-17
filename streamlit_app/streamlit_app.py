import sys
import traceback
from pathlib import Path

import streamlit as st

sys.path.insert(0, '..')

from sequence_processor.sequence import SequenceProcessor
from data_tools import get_databases, load_data, get_group
from graph_tools import evolution_to_graphviz
from st_tools import check_password


def get_pdf_file(graph):
    graph.render('streamlit_app/figures/Digraph.gv.pdf').replace('\\', '/')
    with open("streamlit_app/figures/Digraph.gv.pdf", "rb") as pdf_file:
        pdf_file = pdf_file.read()

    return pdf_file


def app_main_loop():
    dbs = get_databases(Path("../data/"))
    dbs_dict = {db.name: db for db in dbs}
    db_option = st.selectbox('Select database', dbs_dict.keys())
    df = load_data(dbs_dict.get(db_option))

    st.write('You selected:', dbs_dict.get(db_option))
    st.dataframe(df)

    kernel_option = st.selectbox('Select `kernel_id`', df.kernel_id.unique())
    df_group = get_group(df, "kernel_id", kernel_option)

    try:
        processor = SequenceProcessor(df_group)
        snap_num = st.slider('Snapshot number', 0, len(processor.snapshots) - 1, 0)
        snap = processor.snapshots[snap_num]
        st.warning(snap.index_order)

        g_gv = evolution_to_graphviz(processor, snap_num + 1)
        st.graphviz_chart(g_gv)

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button('Prepare graph file', use_container_width=True):
                disabled = False
            else:
                disabled = True
                # graph_pdf = get_pdf_file(g_gv)
                # images = convert_from_bytes(graph_pdf)

        with col2:
            st.button('Download PDF', disabled=disabled, use_container_width=True)
            # st.download_button(
            #         label="Download graph as png",
            #         data=images,
            #         file_name=f'graph_snapshot_num_{snap_num}.png',
            #         mime='text/csv',
            # )

        with col3:
            st.button('Download PNG', disabled=disabled, use_container_width=True)
            # st.download_button(
            #     label="Download graph as PDF",
            #     data=graph_pdf,
            #     file_name=f'graph_snapshot_num_{snap_num}.pdf',
            #     mime='application/octet-stream',
            # )

        st.header("Event")
        st.warning(snap.log)

        st.header("Notebook cells")
        for (cell_index, cell_num) in snap.index_order:
            st.code(f"# {cell_index}, {cell_num}\n\n{snap.index_source_mapping[cell_index]}")

        # g_nx = evolution_to_networkx(evol, snap_num)
        # fig, ax = draw_nx_graph(g_nx)
        # st.pyplot(fig)

    except Exception as e:
        st.error(traceback.format_exc())
        st.warning("Something wrong with notebook evolution")


if __name__ == '__main__':
    authorize = check_password()
    authorize = True
    if authorize:
        app_main_loop()
