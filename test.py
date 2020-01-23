from datetime import date

from sqlalchemy import Column, String, Integer, Date, Numeric
from base import session_factory, Base


class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    date_of_birth = Column(Date)
    height = Column(Integer)
    weight = Column(Numeric)

    def __init__(self, name, date_of_birth, height, weight):
        self.name = name
        self.date_of_birth = date_of_birth
        self.height = height
        self.weight = weight

def create_people():
    session = session_factory()
    bruno = Person("Bruno Krebs", date(1984, 10, 20), 182, 84.5)
    john = Person("John Doe", date(1990, 5, 17), 173, 90)
    session.add(bruno)
    session.add(john)
    session.commit()
    session.close()


def get_people():
    session = session_factory()
    people_query = session.query(Person)
    session.close()
    return people_query.all()


if __name__ == "__main__":
    people = get_people()
    if len(people) == 0:
        create_people()
    people = get_people()

    for person in people:
        print('{} was born in {}'.format(person.name, person.date_of_birth))
