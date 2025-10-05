import pytest

from models import Photo
from filters import apply_photo_filter

from datetime import datetime

# Assume `Photo` and SQLAlchemy Base are already imported from your models.


def make_test_photos():
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

    # ---- Within "Last Year" range: 2024-05-01 .. 2025-07-10 ----
    photos.append(P(datetime(2024, 5, 10, 14, 30, 0),  # P02
                    keywords=["spring", "family"],
                    rating=5,
                    label_color="Red",
                    camera="Fuji X-T5", iso=320))
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

    return photos

# Example: add to a session for testing
# session.add_all(make_test_photos())
# session.commit()


@pytest.fixture
def add_photos(session):
    photos = make_test_photos()
    session.add_all(photos)
    session.commit()
    yield
    session.query(Photo).delete()
    session.commit()


def test_date_photo_filter(config, session, add_photos):

    filter = [slideshow["filter"] for slideshow in config.slideshows if slideshow["name"] == "Last Year"][0]

    # Example: get all Photos matching default_filter
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 4
    for photo in result:
        assert datetime(2024, 5, 1) <= photo.timestamp_id <= datetime(2025, 4, 30)

def test_keywords_photo_filter(config, session, add_photos):

    # Example 1 with uppercase filter to lowercase keyword
    filter = [slideshow["filter"] for slideshow in config.slideshows if slideshow["name"] == "Asia 2014"][0]
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 2
    for photo in result:
        assert "asia 2014" in [x.lower() for x in photo.keywords]

    # Example 2 with lowercase filter to uppercase keyword
    filter = [slideshow["filter"] for slideshow in config.slideshows if slideshow["name"] == "Mountains"][0]
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 3
    for photo in result:
        assert "mountain" in [x.lower() for x in photo.keywords] or "mountains" in [x.lower() for x in photo.keywords]

def test_date_part_photo_filter(config, session, add_photos):

    filter = [slideshow["filter"] for slideshow in config.slideshows if slideshow["name"] == "Christmas"][0]

    # Example: get all Photos matching date_part filter
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 4
    for photo in result:
        assert photo.timestamp_id.month == 12

def test_rating_photo_filter(config, session, add_photos):

    filter = [slideshow["filter"] for slideshow in config.slideshows if slideshow["name"] == "best"][0]

    # Example: get all Photos matching rating filter
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 4
    for photo in result:
        assert photo.rating >= 5

def test_labels_photo_filter(config, session, add_photos):

    filter = config.default_filter

    # Example: get all Photos matching labels filter
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 1
    for photo in result:
        assert photo.label_color == "Red"

def test_combined_photo_filter(config, session, add_photos):

    filter = [slideshow["filter"] for slideshow in config.slideshows if slideshow["name"] == "Last year mountains"][0]

    # Example: get all Photos matching combined filter
    q = session.query(Photo)
    q = apply_photo_filter(q, dialect_name=session.bind.dialect.name, filter_dict=filter)
    result = q.all()
    assert len(result) == 1
    assert result[0].timestamp_id == datetime(2024, 11, 2, 9, 12, 0)