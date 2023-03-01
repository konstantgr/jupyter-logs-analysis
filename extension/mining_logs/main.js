define([
    'jquery',
    'base/js/namespace',
    'base/js/events',
    ], function($, Jupyter, events) {
    "use strict";

    function sendRequest(json_data) {
        fetch("http://3.249.245.244:9999/", {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: json_data
        })
            .then(response => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then(json_data => {
                console.log(json_data);
            })
            .catch(error => {
                console.error('There was a problem with the request:', error);
            });
    }

    function saveLogs(time, sessionId, kernelId, notebookName, event, cell, cellNumber) {
        console.log(cell, cell === undefined)
        if (cell === undefined) {
            var cellSource = "";
            var cellIndex = "";
        }
        else {
            var cellSource = cell.get_text();
            var cellIndex = cell.cell_id;
        }
        var logs = {
            "time": time,
            "kernel_id": kernelId,
            "notebook_name": notebookName,
            "event": event,
            "cell_index": cellIndex,
            "cell_num": cellNumber,
            "cell_source": cellSource,
            "session_id": sessionId
        };

        sendRequest(JSON.stringify(logs));
    }

    function registerEvents() {
        const tracked_events = [
            'create.Cell',
            'delete.Cell',
            'execute.CodeCell',
            'rendered.MarkdownCell',
            'notebook_renamed.Notebook',
            'kernel_interrupting.Kernel',
            'kernel_restarting.Kernel',
        ];

        events.on(tracked_events.join(' '), function(evt, data) {
            const kernelId = Jupyter.notebook.kernel.id;
            const notebookName = Jupyter.notebook.notebook_name;
            const cellNumber = Jupyter.notebook.get_selected_index();
            const cell = data.cell
            const sessionId = Jupyter.notebook.session.id

            saveLogs((new Date()).toISOString(), sessionId, kernelId, notebookName, evt.type, cell, cellNumber);
        });
    }

    function DeleteUpAndDownButtons(){
        var btn_up = document.querySelector('.btn.btn-default[title="move selected cells up"]');
        var btn_down = document.querySelector('.btn.btn-default[title="move selected cells down"]');

        if (btn_up) {
            btn_up.parentNode.removeChild(btn_up);
        }
        if (btn_down) {
            btn_down.parentNode.removeChild(btn_down);
        }
    }

    function loadExtension() {
        if (Jupyter.notebook) {
            registerEvents();
            DeleteUpAndDownButtons();

        } else {
            events.on('notebook_loaded.Notebook', function() {
                registerEvents();
                DeleteUpAndDownButtons();

            });
        }
    }

    return {
        load_ipython_extension: loadExtension
    };
});