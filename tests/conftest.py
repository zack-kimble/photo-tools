import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from main import Config
from models import Base, Photo, PhotoSourceFile
from datetime import datetime

@pytest.fixture
def config():
    return Config.load_from_yaml("tests/test_assets/test_config.yaml")

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

@pytest.fixture
def test_photo_abs_path():
    return Path('tests/test_assets/_DSC2510.jpg').resolve()

@pytest.fixture
def test_photo_abs_path_made_relative(test_photo_abs_path):
    return test_photo_abs_path.relative_to('/')





def make_test_photos(test_photo_abs_path):
    """
    Returns a list[Photo] with varied timestamps, keywords, and ratings.
    Coverage:
      - "Last Year" (2024-05-01 .. 2025-07-10): P02, P03, P04, P05
      - "Asia 2014" (keywords contains "Asia 2014"): P06, P07
      - "Mountains" (keywords contains "mountain" or "mountains"): P03, P08, P09
      - "Christmas" (month == 12): P07, P10, P11, P12
      - "best" (rating == 5): P02, P08, P12, P13
    Many others intentionally do not match each query.
    """
    photos = []

    # Helper for concise object creation
    def P(ts, *, keywords=None, rating=None, label_color=None, **meta):
        return Photo(
            timestamp_id=ts,
            exif_metadata=meta or {"camera": "Nikon", "iso": 200},
            label_color=label_color or "none",
            rating=rating,
            keywords=list(keywords or []),
        )

    psf = PhotoSourceFile(
        absolute_path_id=str(test_photo_abs_path), timestamp=datetime(2024, 5, 10, 14, 30, 0))

    # ---- Within "Last Year" range: 2024-05-01 .. 2025-07-10 ----
    p1 =P(datetime(2024, 5, 10, 14, 30, 0),  # P02
                    keywords=["spring", "family"],
                    rating=5,
                    label_color="Red",
                    camera="Fuji X-T5", iso=320)
    psf.photos = p1
    p1.reference_source_file = psf.absolute_path_id
    photos.append(p1)
    photos.append(P(datetime(2024, 11, 2, 9, 12, 0),   # P03
                    keywords=["hike", "mountain"],
                    rating=3,
                    camera="Nikon D800", iso=400))
    photos.append(P(datetime(2025, 1, 15, 18, 5, 0),   # P04
                    keywords=["city", "night"],
                    rating=4,
                    camera="iPhone", iso=125))
    photos.append(P(datetime(2025, 4, 1, 7, 0, 0),     # P05
                    keywords=["summer", "beach"],
                    rating=2,
                    camera="Ricoh GR", iso=100))

    # ---- Asia 2014 (keyword must include "Asia 2014") ----
    photos.append(P(datetime(2014, 6, 15, 10, 0, 0),   # P06
                    keywords=["Asia 2014", "temple", "street"],
                    rating=4,
                    camera="Nikon D700", iso=200))
    photos.append(P(datetime(2014, 12, 24, 20, 45, 0), # P07 (also Christmas by month)
                    keywords=["asia 2014", "market", "night"],
                    rating=3,
                    camera="Nikon D700", iso=800))

    # ---- Mountains (keywords: "mountain" or "mountains") ----
    photos.append(P(datetime(2023, 9, 10, 6, 30, 0),   # P08
                    keywords=["landscape", "Mountains", "sunrise"],
                    rating=5,
                    camera="Nikon Z7", iso=64))
    photos.append(P(datetime(2022, 2, 19, 12, 0, 0),   # P09
                    keywords=["ski", "mountain"],
                    rating=4,
                    camera="Sony A7C", iso=200))

    # ---- Christmas (month == 12, any year) ----
    photos.append(P(datetime(2018, 12, 25, 9, 0, 0),   # P10
                    keywords=["family", "tree"],
                    rating=4,
                    camera="Canon 5D", iso=400))
    photos.append(P(datetime(2023, 12, 5, 19, 30, 0),  # P11
                    keywords=["city", "lights"],
                    rating=3,
                    camera="iPhone", iso=250))
    photos.append(P(datetime(2025, 12, 1, 8, 15, 0),   # P12 (also best)
                    keywords=["winter", "snow"],
                    rating=5,
                    camera="Nikon Zf", iso=200))

    # ---- Best (rating == 5) — include some not matching others ----
    photos.append(P(datetime(2021, 3, 3, 15, 45, 0),   # P13
                    keywords=["portrait"],
                    rating=5,
                    camera="Fujifilm X100V", iso=160))

    # ---- Negatives / fillers (don’t match any filter) ----
    photos.append(P(datetime(2013, 4, 2, 11, 11, 0),   # P14
                    keywords=["garden", "macro"],
                    rating=2,
                    camera="Olympus E-M5", iso=200))
    photos.append(P(datetime(2025, 8, 20, 13, 25, 0),  # P15 (outside Last Year end_date, not Dec)
                    keywords=["park", "dog"],
                    rating=1,
                    camera="iPhone", iso=50))
    photos.append(P(datetime(2020, 1, 10, 10, 10, 0),  # P16
                    keywords=["indoor", "product"],
                    rating=3,
                    camera="Sony RX100", iso=200))

    return [psf], photos

# Example: add to a session for testing
# session.add_all(make_test_photos())
# session.commit()


@pytest.fixture
def add_photos(session, test_photo_abs_path):
    psf, photos = make_test_photos(test_photo_abs_path)
    session.add_all(psf+photos)
    session.commit()
    yield
    session.query(Photo).delete()
    session.commit()