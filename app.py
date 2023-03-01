import os

from flask import request, Flask, render_template
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from db_structures import UserLogs

basedir = os.path.abspath(os.path.dirname(__file__))
db_name = 'data/user_activity.db'
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
        "time", "session_id", "kernel_id",
        "notebook_name", "cell_index", "cell_num",
        "event", "cell_source"
    )

    return render_template('db_template.html', rows=rows, columns=columns)


@app.route('/', methods=['GET', 'POST'])
def add_message():
    content = request.json
    print(content)

    user_log = UserLogs(**content)
    db.session.add(user_log)
    db.session.commit()
    return content


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9999)
