import pytest

from models import Photo
from filters import apply_photo_filter
from datetime import datetime



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