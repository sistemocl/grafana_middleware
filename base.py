from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.sql import func

engine = create_engine('postgresql://postgres:postgres@localhost:5432/test')
if not database_exists(engine.url):
    create_database(engine.url)
# The base class which our objects will be defined on.
Base = declarative_base()

MONITORED_PPS = [37,40,43]


# Our User object, mapped to the 'users' table
class PPSTask(Base):
    __tablename__ = 'pps_tasks'
    id = Column(Integer, primary_key=True)

    task_id = Column(String)
    pps = Column(Integer)
    status = Column(String)
    # Time stamps
    ts_created = Column(DateTime, default=func.now())
    ts_pending = Column(DateTime)
    ts_rack_picked = Column(DateTime)
    ts_queue = Column(DateTime)
    ts_storing = Column(DateTime)
    ts_complete = Column(DateTime)
    # Time stamp mapping
    ts_mapping = {
        "created": "ts_created",
        "pending": "ts_pending",
        "rack_picked": "ts_rack_picked",
        "pps_queue": "ts_queue",
        "storing": "ts_storing",
        "complete": "ts_complete"
    }

    def update_ts(self,task_state):
        setattr(self,PPSTask.ts_mapping[task_state],func.now())

    def update_from_task(self, new_task):
        # Check for no update
        if self.status == new_task.status:
            print("{} not updated".format(self))
        else:
            print("{} updated to {}".format(self, new_task.status))
            self.status = new_task.status
            self.update_ts(new_task.status)

    # Print task
    def __repr__(self):
       return "<PPSTask(task_id='{}', pps='{}', status='{}')>".format(self.task_id, self.pps, self.status)


# Create all tables by issuing CREATE TABLE commands to the DB.
Base.metadata.create_all(engine)

# Creates a new session to the database by using the engine we described.
Session = sessionmaker(bind=engine)
session = Session()


class ButlerStatus:

    def __init__(self):
        self.butler_status = {}
        self.task_mapping = {}
        self.pps_queue_mapping = {}

        # Mock
        self.butler_status = {
            1000: {"position":"120.130", "task_id":"p1"},
            1001: {"position": "120.130", "task_id": "p2"}
        }
        self.pps_queue_mapping = {
            37: ["120.130"]
        }

    def _update_task_mapping(self):
        task_mapping = {}
        for butler_id in self.butler_status:
            butler = self.butler_status[butler_id]
            task_mapping[butler["task_id"]] = butler
        self.task_mapping = task_mapping
        print(task_mapping)

    def update_butler_status(self):
        # Make request

        # Update task mapping
        self._update_task_mapping()

    def check_queue_condition(self, task):
        if task.task_id in self.task_mapping:
            butler = self.task_mapping[task.task_id]
            return butler["position"] in self.pps_queue_mapping[task.pps]
        return False


new_p1 = PPSTask(task_id="p2",pps=37,status="pending")
task_p1 = session.query(PPSTask).filter(PPSTask.task_id == new_p1.task_id).first()

butler_status = ButlerStatus()
butler_status.update_butler_status()

if task_p1:
    # Check for pending on queue
    if new_p1.status == "pending" and butler_status.check_queue_condition(new_p1):
        new_p1.status = "pps_queue"
    task_p1.update_from_task(new_p1)
else:
    print("{} new task".format(new_p1))
    session.add(new_p1)

session.commit()