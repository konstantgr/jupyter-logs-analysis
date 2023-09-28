from pathlib import Path

from sqlalchemy import Column, String, Integer, Text
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

from server import MAIN_FOLDER

base = declarative_base()


class UserLogs(base):
    __tablename__ = 'user_logs'

    id = Column(Integer, primary_key=True)
    ip_address = Column(String(50))
    time = Column(String(50))
    session_id = Column(String(50), default=None)
    kernel_id = Column(String(50))
    notebook_name = Column(String(50))
    event = Column(String(50))
    cell_index = Column(String(50))
    cell_num = Column(Integer)
    cell_type = Column(String(50))
    cell_source = Column(Text)
    cell_output = Column(Text)

    def as_dict(self):
        return {
            c.name: getattr(self, c.name)
            for c in self.__table__.columns
        }


def create_db(db_path: Path):
    engine = create_engine(f"sqlite:///{str(db_path)}", echo=True)
    if not database_exists(engine.url):
        create_database(engine.url)

    base.metadata.drop_all(engine)
    base.metadata.create_all(engine)


if __name__ == '__main__':
    create_db(MAIN_FOLDER / "data/test_db.db")
