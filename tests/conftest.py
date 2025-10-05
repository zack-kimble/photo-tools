import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from main import Config
from models import Base

@pytest.fixture
def config():
    return Config.load_from_yaml("config.yaml")

@pytest.fixture
def db_url():
    return "sqlite:///tests/db/test_database.db"

@pytest.fixture
def engine(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    yield engine
    os.remove("tests/db/test_database.db")

@pytest.fixture
def session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
