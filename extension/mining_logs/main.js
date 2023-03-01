define([
    'base/js/namespace',
    'base/js/events'
], function(Jupyter, events) {
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

    function saveLogs(time, kernelId, notebookName, event, cell, cellNumber) {
        const cellSource = cell.get_text();
        const cellIndex = cell.cell_id;

        var logs = {
            "time": time,
            "kernel_id": kernelId,
            "notebook_name": notebookName,
            "event": event,
            "cell_index": cellIndex,
            "cell_num": cellNumber,
            "cell_source": cellSource,
        };

        sendRequest(JSON.stringify(logs));
    }

    function registerEvents() {
        const tracked_events = [
            'create.Cell',
            'delete.Cell',
            'execute.CodeCell',
            'rendered.MarkdownCell'
        ];

        events.on(tracked_events.join(' '), function(evt, data) {
            const kernelId = Jupyter.notebook.kernel.id;
            const notebookName = Jupyter.notebook.notebook_name;
            const cellNumber = Jupyter.notebook.get_selected_index();
            const cell = data.cell

            saveLogs((new Date()).toISOString(), kernelId, notebookName, evt.type, cell, cellNumber);
        });
    }

    function loadExtension() {
        if (Jupyter.notebook) {
            registerEvents();
        } else {
            events.on('notebook_loaded.Notebook', function() {
                registerEvents();
            });
        }
    }

    return {
        load_ipython_extension: loadExtension
    };
});