from pathlib import Path
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

base = declarative_base()


class UserLogs(base):
    __tablename__ = 'user_logs'

    id = Column(Integer, primary_key=True)
    time = Column(String(50))
    session_id = Column(String(50), default=None)
    kernel_id = Column(String(50))
    notebook_name = Column(String(50))
    event = Column(String(50))
    cell_index = Column(String(50))
    cell_num = Column(Integer)
    cell_source = Column(Text)

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
    create_db(Path("data/test_db.db"))
