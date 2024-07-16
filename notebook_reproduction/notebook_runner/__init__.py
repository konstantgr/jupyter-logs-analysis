PROGRESS_EXECUTION_ELEMENT: str = ".//*[contains(text(), '[*]') or contains(text(), 'In [*]')]"
INSERT_CELL_BELOW_ELEMENT: str = '//button[@data-command="notebook:insert-cell-below"]'
CELL_ELEMENT: str = (
    "//div[contains(@class, 'lm-Widget') and contains(@class, 'jp-Cell') "
    "and contains(@class, 'jp-CodeCell') and contains(@class, 'jp-Notebook-cell')]"
)
LAST_CELL_ELEMENT: str = "//div[contains(@class, 'jp-Notebook-cell')][last()]"
CELL_EDIT_AREA: str = ".//div[@class='cm-content']"
CELL_OUTPUT_AREA: str = ".//div[contains(@class, 'jp-OutputArea-output')]"
INTERRUPT_KERNEL_SELECTOR: str = "div.lm-Widget.jp-Toolbar.jp-NotebookPanel-toolbar > div:nth-child(7) > button"

TRACEBACK_MESSAGE: str = "Traceback (most recent call last)"
