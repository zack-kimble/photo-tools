import logging
import os
from datetime import datetime
from pathlib import Path

import pytest
from exiftool import ExifToolHelper
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
    select_reference_source,
    update_photos,
    Base,
    Photo,
    PhotoSourceFile, update_photo
)

# --- Fixtures ---

@pytest.fixture
def image_path_google(tmp_path):
    return Path("tests/test_assets/PXL_20240606_092452536.MP.jpg")

@pytest.fixture
def image_path_nikon(tmp_path):
    return Path("tests/test_assets/_DSC8948.JPG")

@pytest.fixture
def image_path_adobe(tmp_path):
    return Path("tests/test_assets/_DSC2510.jpg")

@pytest.fixture
def image_path_moto(tmp_path):
    return Path("tests/test_assets/IMG_20191124_193029019.jpg")

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

@pytest.fixture
def valid_exif_blue():
    return {
        "Composite:SubSecCreateDate": "2023:03:15 12:30:45",
        "XMP:Rating": 3,
        "XMP:Label": "blue"
    }
@pytest.fixture
def valid_exif_red():
    return {
        "Composite:SubSecCreateDate": "2023:03:15 12:30:45",
        "XMP:Rating": 1,
        "XMP:Label": "red"
    }


@pytest.fixture
def invalid_exif():
    return {
        "XMP:Rating": 2,
    }

@pytest.fixture
def exif_tool_helper():
    exif_tool_helper = ExifToolHelper()
    yield exif_tool_helper
    del exif_tool_helper


# --- Tests ---

def test_extract_exif_data_nikon(image_path_nikon, exif_tool_helper):
    exif_data = extract_exif_data(image_path_nikon,exif_tool_helper)
    assert "XMP:Rating" in exif_data
    assert "XMP:Label" not in exif_data
    assert "Composite:SubSecCreateDate" in exif_data

def test_extract_exif_data_google(image_path_google,exif_tool_helper):
    exif_data = extract_exif_data(image_path_google,exif_tool_helper)
    assert "XMP:Rating" in exif_data
    assert "XMP:Label" in exif_data
    assert "Composite:SubSecCreateDate" in exif_data

def test_extract_exif_data_adobe(image_path_adobe,exif_tool_helper):
    exif_data = extract_exif_data(image_path_adobe,exif_tool_helper)
    assert "XMP:Rating" not in exif_data
    assert "XMP:Label" in exif_data
    assert "Composite:SubSecCreateDate" in exif_data

def test_extract_exif_data_moto(image_path_moto,exif_tool_helper):
    exif_data = extract_exif_data(image_path_moto,exif_tool_helper)
    assert "XMP:Rating" not in exif_data
    assert "XMP:Label"  not in exif_data
    assert "EXIF:DateTimeOriginal" in exif_data



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
    expected = {subdir1, subdir2, dir2}
    assert set(leaves) == expected

def test_normalize_valid_exif_data(config, valid_exif_blue, invalid_exif):
    # Valid datetime string from first dt_key.
    normalized = normalize_exif_data(valid_exif_blue, config.possible_metadata_keys)
    assert isinstance(normalized["timestamp"], str)
    assert normalized["rating"] == 3
    assert normalized["label_color"] == "blue"


def test_normalize_invalid_exif_data(config, invalid_exif, caplog):
    # Set the capture level to WARNING.
    with caplog.at_level(logging.DEBUG):
        normalized_invalid = normalize_exif_data(invalid_exif, config.possible_metadata_keys)

    # Assert that the result is as expected.
    assert normalized_invalid["timestamp"] is None

    # Check that a warning message was logged.
    assert "Missing key timestamp in normalized exif data" in caplog.text


def test_select_reference_source(config):
    # Create a dummy Photo with two PhotoSourceFile objects.
    ts = str(datetime.utcnow())
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)
    psf1 = PhotoSourceFile(absolute_path_id="/media/zack/something/lr_edited_jpgs/My Pictures/D800/image1.jpg")
    psf2 = PhotoSourceFile(absolute_path_id="/media/zack/WD 4TB/My Pictures/D800/image1.jpg")
    photo.source_files = [psf1, psf2]
    selected = select_reference_source(photo, config.source_file_preference)
    assert selected == psf1

def test_update_photo(config, valid_exif_blue, valid_exif_red):
    # Create a Photo with two associated PhotoSourceFile objects.
    ts = valid_exif_blue['Composite:SubSecCreateDate']
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)
    psf1 = PhotoSourceFile(
        absolute_path_id="/media/zack/something/lr_edited_jpgs/My Pictures/D800/image1.jpg",
        rating=1,
        label_color="red",
        timestamp=ts,
        exif_metadata=valid_exif_red
    )
    psf2 = PhotoSourceFile(
        absolute_path_id="/media/zack/WD 4TB/My Pictures/D800/image1.jpg",
        rating=3,
        label_color="blue",
        timestamp=ts,
        exif_metadata=valid_exif_blue
    )
    photo.source_files = [psf1, psf2]
    update_photo(photo, source_file_preference=config.source_file_preference)

    # According to sample preferences, psf1 should be selected.
    assert photo.reference_source_file == psf1.absolute_path_id
    # Check that merged EXIF metadata and other fields come from the reference.
    assert photo.exif_metadata["XMP:Rating"] == 1
    assert photo.label_color == "red"
    assert photo.rating == 1

    #ensure order doesn't change reference file being last update
    photo.source_files = photo.source_files[::-1]
    update_photo(photo, source_file_preference=config.source_file_preference)

    assert photo.reference_source_file == psf1.absolute_path_id
    # Check that merged EXIF metadata and other fields come from the reference.
    assert photo.exif_metadata["XMP:Rating"] == 1
    assert photo.label_color == "red"
    assert photo.rating == 1


def test_process_file(tmp_path, monkeypatch, config, valid_exif_red):
    # Create a dummy file in the temporary directory.
    dummy_file = tmp_path / "dummy.jpg"
    dummy_file.write_text("dummy image content")

    # Monkey-patch extract_exif_data to return a fixed EXIF dictionary.
    monkeypatch.setattr("main.extract_exif_data", lambda file_path, et: valid_exif_red)

    psf = process_file(dummy_file, config, exif_tool_helper=None)
    # Verify that a PhotoSourceFile entry was created.

    assert psf is not None
    assert psf.rating == 1
    assert psf.label_color == "red"
    # # Verify that an associated Photo was created.
    # photo = session.get(Photo, psf.timestamp)
    # assert photo is not None
    # assert psf in photo.source_files

# def test_process_directory(tmp_path, monkeypatch, config, valid_exif_red, engine, db_url):
#     # Create a temporary directory with one allowed file.
#     test_dir = tmp_path / "test_dir"
#     test_dir.mkdir()
#     dummy_file = test_dir / "dummy.jpg"
#     dummy_file.write_text("dummy image content")
#
#     # Monkey-patch extract_exif_data to return a fixed EXIF dictionary.
#     monkeypatch.setattr("main.extract_exif_data", lambda file_path, et: valid_exif_red)
#
#
#     Session = sessionmaker(bind=engine)
#     session = Session()
#
#     process_directory(test_dir, db_url, config)
#
#     psf = session.query(PhotoSourceFile).filter_by(absolute_path_id=str(dummy_file)).first()
#     assert psf is not None
#     session.close()





# --- Test Cases ---

def test_update_photo_normal(config, valid_exif_blue, valid_exif_red):
    """
    Test that update_photo selects the reference file according to the preference,
    and that the photo's attributes (reference_source_file, label_color, rating, exif_metadata)
    are updated correctly.
    """
    ts = valid_exif_blue["Composite:SubSecCreateDate"]
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)

    # psf1 has a path containing 'lr_edited' so it should be selected as reference.
    psf1 = PhotoSourceFile(
        absolute_path_id="/media/zack/something/lr_edited_jpgs/Image1.jpg",
        rating=1,
        label_color="red",
        timestamp=ts,
        exif_metadata=valid_exif_red
    )
    # psf2 has a different path.
    psf2 = PhotoSourceFile(
        absolute_path_id="/media/zack/WD 4TB/MyPictures/Image1.jpg",
        rating=3,
        label_color="blue",
        timestamp=ts,
        exif_metadata=valid_exif_blue
    )
    photo.source_files = [psf1, psf2]
    update_photo(photo, config.source_file_preference)

    # Expect psf1 to be chosen (its path matches "lr_edited").
    assert photo.reference_source_file == psf1.absolute_path_id
    # The photo's label_color and rating should come from psf1.
    assert photo.label_color == psf1.label_color
    assert photo.rating == psf1.rating
    # The merged EXIF metadata is built by merging all source files and then overriding with the reference.
    # In this case, since both have the same datetime and keys, the final result equals psf1.exif_metadata.
    assert photo.exif_metadata == psf1.exif_metadata


def test_update_photo_empty_source_files():
    """
    Test that update_photo does nothing when the Photo has no source_files.
    (Note: the current implementation calls ValueError(...) but does not raise it.)
    """
    ts = "2023:03:15 12:30:45"
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)
    # Call update_photo with an arbitrary preference list.
    update_photo(photo, source_file_preference=["lr_edited"])
    # Expect no updates have been made.
    assert photo.reference_source_file is None
    assert photo.exif_metadata == {}
    assert photo.label_color is None
    assert photo.rating is None


def test_update_photo_no_matching_preference(config, valid_exif_blue):
    """
    Test that if none of the source file paths match any of the given preferences,
    the first source file is chosen as the reference.
    """
    # Modify the config so that the preference string does not occur in the file path.
    config.source_file_preference = ["nonexistent_preference"]
    ts = valid_exif_blue["Composite:SubSecCreateDate"]
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)

    psf1 = PhotoSourceFile(
        absolute_path_id="/media/zack/some/other_path/Image1.jpg",
        rating=2,
        label_color="green",
        timestamp=ts,
        exif_metadata=valid_exif_blue
    )
    photo.source_files = [psf1]
    update_photo(photo, config.source_file_preference)

    # Since no preference matches, the first (and only) source file should be used.
    assert photo.reference_source_file == psf1.absolute_path_id
    assert photo.label_color == psf1.label_color
    assert photo.rating == psf1.rating
    assert photo.exif_metadata == psf1.exif_metadata


def test_update_photo_order_independence(config, valid_exif_blue, valid_exif_red):
    """
    Test that the order of source_files does not affect the outcome.
    The reference file should be selected based solely on the preference criteria.
    """
    ts = valid_exif_blue["Composite:SubSecCreateDate"]
    photo = Photo(timestamp_id=ts, exif_metadata={}, label_color=None, rating=None)

    psf1 = PhotoSourceFile(
        absolute_path_id="/media/zack/something/lr_edited_jpgs/Image1.jpg",
        rating=1,
        label_color="red",
        timestamp=ts,
        exif_metadata=valid_exif_red
    )
    psf2 = PhotoSourceFile(
        absolute_path_id="/media/zack/WD 4TB/MyPictures/Image1.jpg",
        rating=3,
        label_color="blue",
        timestamp=ts,
        exif_metadata=valid_exif_blue
    )
    # Reverse the order of the source files.
    photo.source_files = [psf2, psf1]
    update_photo(photo, config.source_file_preference)

    # psf1 should still be selected as the reference due to its matching substring.
    assert photo.reference_source_file == psf1.absolute_path_id
    assert photo.label_color == psf1.label_color
    assert photo.rating == psf1.rating
    assert photo.exif_metadata == psf1.exif_metadata
