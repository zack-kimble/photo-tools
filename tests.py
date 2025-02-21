import os
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import functions and models from your main module.
# Adjust the import statement if your module name differs.
from main import (
    Config,
    get_leaf_directories,
    extract_exif_data,
    normalize_exif_data,
    process_file,
    process_directory,
    select_reference_source,
    update_photos,
    Base,
    Photo,
    PhotoSourceFile
)

# --- Fixtures ---

@pytest.fixture
def config():
    return Config.load_from_yaml("config.yaml")

@pytest.fixture
def engine():
    # Use an in-memory SQLite database for testing.
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture
def session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


# --- Tests ---

def test_get_leaf_directories(tmp_path):
    # Create a directory structure:
    # tmp_path/dir1/subdir1, tmp_path/dir1/subdir2, and tmp_path/dir2 (leaf directories)
    dir1 = tmp_path / "dir1"
    dir1.mkdir()
    subdir1 = dir1 / "subdir1"
    subdir1.mkdir()
    subdir2 = dir1 / "subdir2"
    subdir2.mkdir()
    dir2 = tmp_path / "dir2"
    dir2.mkdir()

    # Create dummy files so leaves aren't empty.
    (subdir1 / "file1.txt").write_text("hello")
    (subdir2 / "file2.txt").write_text("world")
    (dir2 / "file3.txt").write_text("!")

    leaves = get_leaf_directories(tmp_path)
    # Expected leaves are subdir1, subdir2, and dir2 (dir1 is not a leaf because it contains subdirs).
    expected = {str(subdir1), str(subdir2), str(dir2)}
    assert set(leaves) == expected

def test_normalize_exif_data(config):
    # Valid datetime string from first dt_key.
    exif = {
        "Composite:SubSecCreateDate": "2023:03:15 12:30:45",
        "Rating": 5,
        "Label": "blue"
    }
    normalized = normalize_exif_data(exif, config.possible_metadata_keys)
    assert isinstance(normalized["timestamp"], str)
    assert normalized["rating"] == 5
    assert normalized["label_color"] == "blue"

    # Invalid datetime string should log a warning and result in None.
    exif_invalid = { "Rating": 2}
    normalized_invalid = normalize_exif_data(exif_invalid, config.possible_metadata_keys)
    assert normalized_invalid["timestamp"] is None

def test_select_reference_source():
    # Create a dummy Photo with two PhotoSourceFile objects.
    ts = str(datetime.utcnow())
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)
    psf1 = PhotoSourceFile(absolute_path="/media/zack/WD 4TB/My Pictures/D800/image1.jpg", rating=1)
    psf2 = PhotoSourceFile(absolute_path="/media/zack/WD 4TB/My Pictures/Z7/image2.jpg", rating=2)
    photo.source_files = [psf1, psf2]
    # Given the sample preferences, first is "lr_edited_jpgs" (no match), then "/media/zack/WD 4TB/My Pictures/Z7/"
    preferences = [
        "lr_edited_jpgs",
        "/media/zack/WD 4TB/My Pictures/Z7/",
        "lr_jpgs/My Pictures/D800",
    ]
    selected = select_reference_source(photo, preferences)
    assert selected == psf2

def test_update_photos(session, config):
    # Create a Photo with two associated PhotoSourceFile objects.
    ts = datetime(2023, 3, 15, 12, 30, 45)
    photo = Photo(ts_id=ts, exif_metadata={}, label_color=None, rating=None)
    psf1 = PhotoSourceFile(
        absolute_path="/media/zack/WD 4TB/My Pictures/D800/image1.jpg",
        normalized_exif_metadata={"datetime": ts, "rating": 1, "label_color": "red"},
        rating=1,
        label_color="red",
        photo_ts_id=ts
    )
    psf2 = PhotoSourceFile(
        absolute_path="/media/zack/WD 4TB/My Pictures/Z7/image2.jpg",
        normalized_exif_metadata={"datetime": ts, "rating": 3, "label_color": "blue"},
        rating=3,
        label_color="blue",
        photo_ts_id=ts
    )
    photo.source_files = [psf1, psf2]
    session.add(photo)
    session.commit()

    update_photos(session, config)

    updated_photo = session.query(Photo).filter_by(ts_id=ts).first()
    # According to sample preferences, psf2 (from Z7) should be selected.
    assert updated_photo.reference_source_file == psf2.absolute_path
    # Check that merged EXIF metadata and other fields come from the reference.
    assert updated_photo.exif_metadata["rating"] == 3
    assert updated_photo.label_color == "blue"
    assert updated_photo.rating == 3

def test_process_file(session, tmp_path, monkeypatch, config):
    # Create a dummy file in the temporary directory.
    dummy_file = tmp_path / "dummy.jpg"
    dummy_file.write_text("dummy image content")

    # Monkey-patch extract_exif_data to return a fixed EXIF dictionary.
    def dummy_extract_exif_data(file_path):
        return {
            "Composite:SubSecCreateDate": "2023:03:15 12:30:45",
            "Rating": 4,
            "LabelColor": "green"
        }
    monkeypatch.setattr("photo_processing.extract_exif_data", dummy_extract_exif_data)

    process_file(str(dummy_file), session, config)
    # Verify that a PhotoSourceFile entry was created.
    psf = session.query(PhotoSourceFile).filter_by(absolute_path=str(dummy_file)).first()
    assert psf is not None
    assert psf.rating == 4
    assert psf.label_color == "green"
    # Verify that an associated Photo was created.
    photo = session.query(Photo).filter_by(ts_id=psf.photo_ts_id).first()
    assert photo is not None
    assert psf in photo.source_files

def test_process_directory(tmp_path, monkeypatch, config):
    # Create a temporary directory with one allowed file.
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    dummy_file = test_dir / "dummy.jpg"
    dummy_file.write_text("dummy image content")

    # Monkey-patch extract_exif_data to return a fixed EXIF dictionary.
    def dummy_extract_exif_data(file_path):
        return {
            "Composite:SubSecCreateDate": "2023:03:15 12:30:45",
            "Rating": 2,
            "LabelColor": "yellow"
        }
    monkeypatch.setattr("photo_processing.extract_exif_data", dummy_extract_exif_data)

    # Use an in-memory SQLite database.
    db_url = "sqlite:///:memory:"
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    process_directory(str(test_dir), db_url, config)

    psf = session.query(PhotoSourceFile).filter_by(absolute_path=str(dummy_file)).first()
    assert psf is not None
    session.close()
