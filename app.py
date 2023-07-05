import os
import yaml

from pathlib import Path
from flask import request, Flask, render_template
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from db_structures import UserLogs


with Path("app_config.yaml").open("r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        exit()

basedir = os.path.abspath(os.path.dirname(__file__))
db_name = config['database_rel_path']
host = config['host']
port = config['port']

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{os.path.join(basedir, db_name)}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)


@app.route('/db')
def render_logs_database():
    data = db.session.query(UserLogs).all()
    rows = [d.as_dict() for d in data]
    columns = (
        "ip_address", "time", "session_id", "kernel_id",
        "notebook_name", "cell_index", "cell_num",
        "event", "cell_source", "cell_output"
    )

    return render_template('db_template.html', rows=rows, columns=columns)


@app.route('/', methods=['GET', 'POST'])
def add_message():
    content = request.json
    content['cell_source'] = str(content['cell_source']) if content['cell_source'] else None
    content['cell_output'] = str(content['cell_output']) if content['cell_output'] else None

    user_log = UserLogs(**content)
    db.session.add(user_log)
    db.session.commit()
    return content


if __name__ == "__main__":
    app.run(host=host, port=port)
