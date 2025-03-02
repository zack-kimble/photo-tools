import os
import re
import io
import logging
import sys
import warnings
from datetime import datetime
from multiprocessing import Pool
from sqlite3 import IntegrityError

import numpy as np
from PIL import Image, ExifTags
from exiftool import ExifToolHelper

from sqlalchemy import create_engine, Text
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from models import Base, Photo, PhotoSourceFile
from ruamel.yaml import YAML
yaml = YAML(typ="safe")
from dataclasses import asdict
from pydantic.dataclasses import dataclass
from typing import List, Dict

from pathlib import Path


# --- Logging Configuration ---
logger = logging.getLogger(__name__)


@dataclass
class PossibleMetadataKeys:
    timestamp: List[str]
    label_color: List[str]
    rating: List[str]

@dataclass
class Config:
    original_photo_dirs: List[str]
    file_types: List[str]
    possible_metadata_keys: PossibleMetadataKeys
    source_file_preference: List[str]
    db_url: str = "sqlite:///photos.db"

    @staticmethod
    def load_from_yaml(yaml_str: str) -> "Config":
        with open(yaml_str, "r") as f:
            data = yaml.load(f)
        return Config(**data)



# --- Helper Functions ---

def get_leaf_directories(parent_dir: Path):
    """
    Returns all leaf directories (directories without subdirectories)
    under a given parent directory.
    """
    leaf_dirs = []
    for root, dirs, files in parent_dir.walk():
        if not dirs:
            leaf_dirs.append(root)
    return leaf_dirs
#
# def get_leaf_directories(parent_dir:Path)->List[Path]:
#     # Scan the directory and collect only subdirectories
#     with os.scandir(parent_dir) as entries:
#         subdirs = [entry for entry in entries if entry.is_dir(follow_symlinks=False)]
#
#     # If there are no subdirectories, this is a leaf directory.
#     if not subdirs:
#         return [parent_dir]
#
#     # Otherwise, search recursively in each subdirectory.
#     leaves = []
#     for subdir in subdirs:
#         leaves.extend(get_leaf_directories(subdir.path))
#     return leaves



# def extract_exif_data(file_path):
#     """
#     Extracts EXIF data using Pillow.
#     Returns a dictionary of EXIF data or an empty dict on failure.
#     """
#     image = Image.open(file_path)
#     exif = image._getexif()
#     if exif:
#         # Convert tag IDs to tag names for clarity.
#         exif_data = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
#         return exif_data
#     else:
#         logger.warning(f"No EXIF data found in file: {file_path}")
#         return {}

def extract_exif_data(file_path, exif_tool_helper):
    """
    Extracts EXIF data using PyExifTool.
    Uses the provided ExifTool instance (et) running in batch mode.
    Returns a dictionary of EXIF data or an empty dict on failure.
    """

    # et.get_metadata expects a string path.
    exif_data = exif_tool_helper.get_metadata(str(file_path))
    if exif_data:
        return exif_data[0]
    else:
        logger.warning(f"No EXIF data found in file: {file_path}")
        return {}

def normalize_exif_data(exif: Dict, possible_metadata_keys: PossibleMetadataKeys):
    """
    Normalize key EXIF fields such as datetime, rating, and label color.
    Uses the possible_metadata_keys provided in the config.
    """
    normalized_exif_data = {}
    for normalized_key, possible_source_keys in asdict(possible_metadata_keys).items():
        for key in possible_source_keys:
            if key in exif:
                normalized_exif_data[normalized_key] = exif[key]
                break
    
    missing_keys = set(asdict(possible_metadata_keys).keys()) - set(normalized_exif_data.keys())
    for key in missing_keys:
        normalized_exif_data[key] = None
        logger.debug(f"Missing key {key} in normalized exif data")

    return normalized_exif_data


def process_file(file_path, session, config, exif_tool_helper):
    """
    Process a single file: extracts EXIF data, normalizes it,
    creates a PhotoSourceFile and associated Photo if necessary.
    """

    exif_data = extract_exif_data(file_path, exif_tool_helper)
    normalized_exif = normalize_exif_data(exif_data, config.possible_metadata_keys)
    timestamp_id = normalized_exif.get("timestamp")
    if not timestamp_id:
        warnings.warn(f"Missing datetime for file {file_path}. Unable to create PhotoSourceFile")
        return
    photo_source_file = PhotoSourceFile(
        absolute_path_id=str(file_path),
        exif_metadata=exif_data,
        label_color=normalized_exif.get("label_color"),
        rating=normalized_exif.get("rating"),
        timestamp=timestamp_id
    )
    photo_source_file = session.merge(photo_source_file)

    # # Get or create associated Photo using timestamp_id as key.
    # photo = session.get(Photo, timestamp_id)
    # if not photo:
    #     photo = Photo(timestamp_id=timestamp_id, exif_metadata={}, label_color=None, rating=None)
    #     session.merge(photo)

    try:
        # Try to get or create the Photo.
        photo = session.get(Photo, timestamp_id)
        if not photo:
            photo = Photo(timestamp_id=timestamp_id, exif_metadata={}, label_color=None, rating=None)
            session.add(photo)
            session.commit()  # commit immediately to ensure it's visible to other sessions
    except IntegrityError:
        # Another worker created the Photo concurrently. Fetch it.
        session.rollback()

    # if photo_source_file not in photo.source_files:
    #     photo.source_files.append(photo_source_file)


def process_directory(leaf_dir: Path, db_url, config: Config):
    """
    Processes all files in a given leaf directory.
    Filters files based on allowed file types from config.
    Each process creates its own SQLAlchemy session.
    """

    engine = create_engine(db_url,
        connect_args={'check_same_thread': False},
        poolclass=NullPool
    )
    # Optionally, set WAL mode to improve write concurrency:
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        connection.exec_driver_sql("PRAGMA synchronous=normal")
        connection.exec_driver_sql("PRAGMA busy_timeout = 10000")
    # Disable autoflush to avoid premature flushing during queries
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()
    allowed_types = [ft.lower() for ft in config.file_types]
    try:
        with ExifToolHelper() as exif_tool_helper:
            for entry in leaf_dir.iterdir():
                full_path = leaf_dir.joinpath(entry)
                if full_path.is_file() and full_path.suffix.lower() in allowed_types:
                    process_file(full_path, session, config, exif_tool_helper)
                else:
                    logging.debug(f"Skipping file {full_path} (unsupported type: {full_path.suffix})")
       # session.commit()
        logger.info(f"Processed directory {leaf_dir}")
    except Exception as e:
        session.rollback()
        raise ValueError(f"Error processing directory {leaf_dir}: {e}")
    finally:
        session.close()


def select_reference_source(photo, preferences):
    """
    Given a Photo with multiple source files, select the reference source file
    based on the ordered preference list. For each preference, if a source file's
    absolute path contains the preference string, it is selected.
    If none match, return the first source file.
    """
    for pref in preferences:
        for psf in photo.source_files:
            if pref in psf.absolute_path_id:
                return psf
    warnings.warn(f"No reference source file found for photo {photo.timestamp_id}. Using first source file.")
    return photo.source_files[0]

def merge_exif_data(photo_source_files: list[PhotoSourceFile], reference_photo_source_file: PhotoSourceFile) -> Dict:
    """Merge EXIF data from all source files, making sure reference file's data takes precedence."""
    merged_exif_data = {}
    for psf in photo_source_files:
        merged_exif_data.update(psf.exif_metadata)
    merged_exif_data.update(reference_photo_source_file.exif_metadata)
    return merged_exif_data

def update_photo(photo: Photo, source_file_preference):
    if not photo.source_files:
        warnings.warn(f"No source files found for photo {photo.timestamp_id}")
        return

    reference_file = select_reference_source(photo, source_file_preference)
    photo.reference_source_file = reference_file.absolute_path_id
    # Use reference file's label_color and rating.
    photo.label_color = reference_file.label_color
    photo.rating = reference_file.rating
    # Merge EXIF data from all source files, making sure reference file's data takes precedence.
    photo.exif_metadata = merge_exif_data(photo.source_files, reference_file)
    return


def update_photos(session: Session, config: Config):
    """
    For each Photo in the database, retrieves its source files,
    selects a reference source file based on config preference order,
    merges EXIF data from source files, and updates the photo's label_color
    and rating.
    """
    preferences = config.source_file_preference
    photos = session.query(Photo).all()
    for photo in photos:
        update_photo(photo, preferences)
        session.commit()

def setup_root_stdout_root_logger():
    # Get the root logger and set its level to INFO
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Loop through all handlers and set their level to INFO
    for handler in root_logger.handlers:
        handler.setLevel(logging.INFO)

    if not root_logger.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)


def main():
    # Load configuration
    config = Config.load_from_yaml("config.yaml")

    setup_root_stdout_root_logger()

    db_url = config.db_url

    # Create the engine and ensure tables exist.
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    logger.info("Database tables created.")
    print("If you see this and no logs, logging isn't being output")
    Session = sessionmaker(bind=engine)
    session = Session()

    # Discover leaf directories from each parent directory in the config.
    leaf_dirs = []
    logger.info("Discovering leaf directories")
    for parent in config.original_photo_dirs:
        leaf_dirs.extend(get_leaf_directories(Path(parent)))
    logger.info(f"Found {len(leaf_dirs)} leaf directories.")

    # Create tasks for each leaf directory and process in parallel.
    tasks = [(leaf, db_url, config) for leaf in leaf_dirs]
    with Pool() as pool:
        pool.starmap(process_directory, tasks)

    # Update photos after processing all directories.
    update_photos(session, config)
    session.close()


if __name__ == "__main__":
    main()


#TODO: refactor to
# 1) Workers compare file updated timestamp vs database updated timestamp to determine if running exiftool to update is necessary.
# 2) have workers collect exiftool output and create photosourcefile object on a queue asynchronously. Have a separeate single worker process write batches of from the queue to outputs to db.