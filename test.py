
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()

class Elevator(Base):
    __tablename__ = 'elevator'
    id = Column(Integer, primary_key=True)

    def __init__(self, id):
        self.id = id

if __name__ == "__main__":
    engine = create_engine('postgresql://postgres:postgres@10.113.95.45:5432/grafana_orm')
    Base.metadata.create_all(engine)
    session = Session()
    elevator =  Elevator(1)
    session.add(elevator)
    session.commit()
    session.close()
