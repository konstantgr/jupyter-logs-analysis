import sys
import traceback
from pathlib import Path

sys.path.insert(0, '..')

import streamlit as st

from data_tools import get_group, preprocess_dataframe
from graph_tools import evolution_to_graphviz
from sequence_processor.sequence import SequenceProcessor
from st_tools import check_password
from analysis.data_loading import read_hackathon_data


def get_pdf_file(graph):
    graph.render('streamlit_app/figures/Digraph.gv.pdf').replace('\\', '/')
    with open("streamlit_app/figures/Digraph.gv.pdf", "rb") as pdf_file:
        pdf_file = pdf_file.read()

    return pdf_file


def main_page_columns():
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

    # g_nx = evolution_to_networkx(evol, snap_num)
    # fig, ax = draw_nx_graph(g_nx)
    # st.pyplot(fig)


def app_main_loop():
    path = Path("../analysis/data_config.yaml")
    df = preprocess_dataframe(read_hackathon_data(path))

    user_option = st.selectbox('Select user', sorted(df.user_id.unique()))
    st.write(f"Selected user: {user_option}")

    df_user = get_group(df, "user_id", user_option)
    kernel_option = st.selectbox('Select `kernel_id`', df_user.kernel_id.unique())

    df_group = get_group(df_user, "kernel_id", kernel_option)

    try:
        processor = SequenceProcessor(df_group)
        snap_num = st.slider('Snapshot number', 0, len(processor.snapshots) - 1, 0)
        snap = processor.snapshots[snap_num]
        # st.warning(snap.index_order)

        st.header("Event")
        st.warning(snap.log)

        plot_graph = st.checkbox('Plot graph')

        if plot_graph:
            g_gv = evolution_to_graphviz(processor, snap_num + 1)
            st.graphviz_chart(g_gv)

        st.header("Notebook cells")
        for (cell_index, cell_num) in snap.index_order:
            st.code(f"# [CELL INDEX]: {cell_index}, [CELL NUM]: {cell_num}\n\n{snap.index_source_mapping[cell_index]}")

    except Exception as e:
        st.error(traceback.format_exc())
        st.warning("Something wrong with notebook evolution")


if __name__ == '__main__':
    authorize = check_password()
    authorize = True
    if authorize:
        app_main_loop()
