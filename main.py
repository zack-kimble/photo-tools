import os
import re
import io
import logging
import warnings
from datetime import datetime
from multiprocessing import Pool

import numpy as np
from PIL import Image, ExifTags

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean, JSON, DateTime, NUMERIC, types
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from models import Base, Photo, PhotoSourceFile
import yaml
from dataclasses import dataclass, field, asdict
from typing import List, Dict


# --- Logging Configuration ---
logger = logging.getLogger(__name__)


@dataclass
class PossibleMetadataKeys:
    timestamp: str
    label_color: str
    rating: str

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
            data = yaml.safe_load(f)
        data["possible_metadata_keys"] = PossibleMetadataKeys(**data.pop("possible_metadata_keys"))
        return Config(**data)



# --- Helper Functions ---

def get_leaf_directories(parent_dir):
    """
    Returns all leaf directories (directories without subdirectories)
    under a given parent directory.
    """
    leaf_dirs = []
    for root, dirs, files in os.walk(parent_dir):
        if not dirs:
            leaf_dirs.append(root)
    return leaf_dirs


def extract_exif_data(file_path):
    """
    Extracts EXIF data using Pillow.
    Returns a dictionary of EXIF data or an empty dict on failure.
    """
    try:
        image = Image.open(file_path)
        exif = image._getexif()
        if exif:
            # Convert tag IDs to tag names for clarity.
            exif_data = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
            return exif_data
        else:
            logger.warning(f"No EXIF data found in file: {file_path}")
    except Exception as e:
        logging.error(f"Error reading EXIF data from file {file_path}: {e}")
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
        logger.warning(f"Missing key {key} in normalized exif data")

    return normalized_exif_data


def process_file(file_path, session, config):
    """
    Process a single file: extracts EXIF data, normalizes it,
    creates a PhotoSourceFile and associated Photo if necessary.
    """
    try:
        exif_data = extract_exif_data(file_path)
        normalized_exif = normalize_exif_data(exif_data, config.possible_metadata_keys)
        timestamp_id = normalized_exif.get("timestamp")
        if not timestamp_id:
            warnings.warn(f"Missing datetime for file {file_path}. Unable to create PhotoSourceFile")
            return
        photo_source_file = PhotoSourceFile(
            absolute_path=file_path,
            exif_metadata=exif_data,
            normalized_exif_metadata=normalized_exif,
            label_color=normalized_exif.get("label_color"),
            rating=normalized_exif.get("rating"),
            timestamp=timestamp_id
        )
        session.merge(photo_source_file)

        # Get or create associated Photo using timestamp_id as key.
        photo = session.query(Photo).filter_by(ts_id=timestamp_id).first()
        if not photo:
            photo = Photo(ts_id=timestamp_id, exif_metadata={}, label_color=None, rating=None)
            session.add(photo)
        if photo_source_file not in photo.source_files:
            photo.source_files.append(photo_source_file)
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")


def process_directory(leaf_dir, db_url, config):
    """
    Processes all files in a given leaf directory.
    Filters files based on allowed file types from config.
    Each process creates its own SQLAlchemy session.
    """
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    allowed_types = [ft.lower() for ft in config.get("file_types", [])]
    try:
        for entry in os.listdir(leaf_dir):
            full_path = os.path.join(leaf_dir, entry)
            if os.path.isfile(full_path):
                ext = os.path.splitext(entry)[1].lower().lstrip(".")
                if ext in allowed_types:
                    process_file(full_path, session, config)
                else:
                    logging.info(f"Skipping file {full_path} (unsupported type: {ext})")
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error processing directory {leaf_dir}: {e}")
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
            if pref in psf.absolute_path:
                return psf
    return photo.source_files[0]


def update_photos(session, config):
    """
    For each Photo in the database, retrieves its source files,
    selects a reference source file based on config preference order,
    merges EXIF data from source files, and updates the photo's label_color
    and rating.
    """
    preferences = config.get("source_file_preference", [])
    photos = session.query(Photo).all()
    for photo in photos:
        if not photo.source_files:
            warnings.warn(f"No source files found for photo {photo.ts_id}")
            continue

        reference_file = select_reference_source(photo, preferences)
        photo.reference_source_file = reference_file.absolute_path

        # Merge EXIF data from all source files (later files override earlier keys)
        merged_exif = {}
        for psf in photo.source_files:
            if psf.normalized_exif_metadata:
                merged_exif.update(psf.normalized_exif_metadata)
        photo.exif_metadata = merged_exif

        # Use reference file's label_color and rating.
        photo.label_color = reference_file.label_color
        photo.rating = reference_file.rating

        session.add(photo)
    session.commit()


# --- Main Execution Flow ---



def main():
    # Load configuration
    config = Config.load_from_yaml("config.yaml")
    db_url = config.get("db_url")

    # Create the engine and ensure tables exist.
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Discover leaf directories from each parent directory in the config.
    leaf_dirs = []
    for parent in config.get("original_photo_dirs", []):
        leaf_dirs.extend(get_leaf_directories(parent))
    logging.info(f"Found {len(leaf_dirs)} leaf directories.")

    # Create tasks for each leaf directory and process in parallel.
    tasks = [(leaf, db_url, config) for leaf in leaf_dirs]
    with Pool() as pool:
        pool.starmap(process_directory, tasks)

    # Update photos after processing all directories.
    update_photos(session, config)
    session.close()


if __name__ == "__main__":
    main()
