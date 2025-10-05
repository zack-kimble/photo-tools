import itertools
import os
import re
import io
import logging
import sys
import warnings
from datetime import datetime
from functools import partial
from multiprocessing import Pool, Process, Queue
from queue import Empty
from sqlite3 import IntegrityError

import numpy as np
from PIL import Image, ExifTags
from exiftool import ExifToolHelper

from sqlalchemy import create_engine, Text, bindparam
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import relationship, sessionmaker, Session, joinedload
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from models import Base, Photo, PhotoSourceFile
from ruamel.yaml import YAML
yaml = YAML(typ="safe")
from dataclasses import asdict
from pydantic.dataclasses import dataclass
from typing import List, Dict, Callable, Any, Optional, Iterable

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
    original_photo_dirs: List[Path]
    file_types: List[str]
    possible_metadata_keys: PossibleMetadataKeys
    source_file_preference: List[str]
    db_url: str = "sqlite:///photos.db"
    batch_size: int = 100
    default_filter: Dict = None
    slideshows: List[Dict[str, Any]] = None


    @staticmethod
    def load_from_yaml(yaml_str: str) -> "Config":
        with open(yaml_str, "r") as f:
            data = yaml.load(f)
        data['original_photo_dirs'] = [Path(p) for p in data['original_photo_dirs']]
        return Config(**data)



# --- Helper Functions ---
from pathlib import Path

def list_files_recursive(path: str|Path, file_types: Optional[list]=None) -> List[Path]:
    """
    Recursively lists files in the given directory.

    Args:
        path (str or Path): The directory path.
        file_types (list, optional): A list of file extensions to filter by (e.g., ['.py', '.txt']).
                                     If None, all files are returned.

    Returns:
        list: A list of file paths as strings.
    """
    p = Path(path)

    # Normalize the file types if provided (ensure each extension starts with a dot and is lowercased)
    if file_types:
        normalized_types = [ext if ext.startswith('.') else f'.{ext}' for ext in file_types]
        normalized_types = [ext.lower() for ext in normalized_types]
        files_found =  [file for file in p.rglob('*')
                if file.is_file() and file.suffix.lower() in normalized_types]
    else:
        files_found = [file for file in p.rglob('*') if file.is_file()]

    logger.info(f"Found {len(files_found)} files in {path}")

    return files_found

def make_lazy_batches(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if not chunk:
            break
        yield chunk



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

def check_file_needs_processing(file_path: Path, photo_source_file) -> bool:
    """
    Check if a file needs processing by comparing its last modified timestamp
    with the last_updated timestamp in the database.
    """

    if not photo_source_file:
        return True  # New file, needs processing

    # Get file's last modified time
    file_modified_time = datetime.fromtimestamp(file_path.stat().st_mtime)

    # If the database record is older than the file's modified time,
    # or if last_updated is None, we need to process it
    if photo_source_file.last_updated is None or file_modified_time > photo_source_file.last_updated:
        return True

    return False  # File hasn't changed since last processing

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

def split_parent_directories(file_path: Path, original_photo_dirs: List[Path]) -> List[str]:
    parents = []
    # Iterate through parents from immediate parent top, excluding root
    for path in file_path.parents[:-1]:
        if path in original_photo_dirs:
            return parents
        else:
            parents.append(path.name)
    raise ValueError(f"File {file_path} is not in any of the original_photo_dirs {original_photo_dirs}")


def add_directory_to_IPTC_keywords(file_path: Path, exif: Dict, original_photo_dirs: List[Path]):
    """
    Adds the directory path of the file to the EXIF tags dictionary.
    """

    directory_keywords = split_parent_directories(file_path,original_photo_dirs)
    current_keywords = exif.get('IPTC:Keywords')
    if isinstance(current_keywords, str):
        directory_keywords.append(current_keywords)
        exif['IPTC:Keywords'] = directory_keywords
    elif isinstance(current_keywords, list):
        for directory in directory_keywords:
            if directory not in current_keywords:
                exif['IPTC:Keywords'].append(directory)
    else:
        exif['IPTC:Keywords'] = directory_keywords
    return

def parse_exif_timestamp(s: str) -> datetime:
    """
    Parse EXIF-like timestamp strings that may include fractional seconds and/or timezone info.
    """
    formats = [
        "%Y:%m:%d %H:%M:%S.%f%z",  # with fraction + tz
        "%Y:%m:%d %H:%M:%S%z",     # without fraction + tz
        "%Y:%m:%d %H:%M:%S.%f",    # with fraction, no tz
        "%Y:%m:%d %H:%M:%S",       # plain
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized timestamp format: {s}")

def process_file(file_path, config, exif_tool_helper) -> PhotoSourceFile | None:
    """
    Process a single file: extracts EXIF data, normalizes it,
    creates a PhotoSourceFile and associated Photo if necessary.
    """

    exif_data = extract_exif_data(file_path, exif_tool_helper)
    normalized_exif = normalize_exif_data(exif_data, config.possible_metadata_keys)
    add_directory_to_IPTC_keywords(file_path=file_path,
                                   exif=normalized_exif,
                                   original_photo_dirs=config.original_photo_dirs)

    timestamp = normalized_exif.get("timestamp")
    if not timestamp:
        warnings.warn(f"Missing datetime for file {file_path}. Unable to create PhotoSourceFile")
        return
    try:
        timestamp_dt = parse_exif_timestamp(timestamp)
    except ValueError:
        warnings.warn(f"Invalid timestamp {timestamp} for file {file_path}. Unable to create PhotoSourceFile")
        return
    photo_source_file = PhotoSourceFile(
        absolute_path_id=str(file_path),
        exif_metadata=exif_data,
        label_color=normalized_exif.get("label_color"),
        rating=normalized_exif.get("rating"),
        keywords=normalized_exif.get("IPTC:Keywords"),
        timestamp=timestamp_dt,
        last_updated=datetime.fromtimestamp(file_path.stat().st_mtime)
    )

    return photo_source_file


def process_batch(files: List[Path], config: Config):
    """
    Process a batch of files, collecting PhotoSourceFile objects.
    """
    # TODO move SLQlite specific details somewhere else
    engine = create_engine(f'{config.db_url}?journal_mode=WAL',
                           connect_args={'check_same_thread': False},
                           poolclass=NullPool
                           )
    # Optionally, set WAL mode to improve write concurrency:
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA busy_timeout = 10000")
        connection.exec_driver_sql("PRAGMA query_only = ON;")

    session = Session(engine)
    try:
        with ExifToolHelper() as exif_tool_helper:
            uncomitted_orm_objects = []
            for file in files:
                db_photo_source_file = session.get(PhotoSourceFile, str(file))
                if check_file_needs_processing(file, db_photo_source_file):
                    photo_source_file = process_file(file_path=file,
                                                     config=config,
                                                     exif_tool_helper=exif_tool_helper)
                    if photo_source_file:
                        uncomitted_orm_objects.append(photo_source_file)
                else:
                    photo_source_file = db_photo_source_file
                    logger.debug(f"Skipping unchanged file {file}")
                if photo_source_file:
                    photo = session.get(Photo, photo_source_file.timestamp)
                    if not photo:
                        photo = Photo(timestamp_id=photo_source_file.timestamp)
                        uncomitted_orm_objects.append(photo)
        return uncomitted_orm_objects
    except Exception as e:
        raise ValueError(f"Error processing directory {files}: {e}")
    finally:
        session.close()

    return uncomitted_photo_source_files

# def collect_updates_from_directory(leaf_dir: Path, db_url, config: Config) -> List[PhotoSourceFile]:
#     """
#     Processes all files in a given leaf directory.
#     Filters files based on allowed file types from config.
#     Each invocation creates its own SQLAlchemy session.
#     """
#     #TODO move SLQlite specific details somewhere else
#     engine = create_engine(db_url,
#         connect_args={'check_same_thread': False},
#         poolclass=NullPool
#     )
#     # Optionally, set WAL mode to improve write concurrency:
#     with engine.connect() as connection:
#         connection.exec_driver_sql("PRAGMA busy_timeout = 10000")
#         connection.exec_driver_sql("PRAGMA query_only = ON;")
#
#     session = Session()
#     allowed_types = [ft.lower() for ft in config.file_types]
#     try:
#         with ExifToolHelper() as exif_tool_helper:
#             uncomitted_photo_source_files = []
#             for entry in leaf_dir.iterdir():
#                 full_path = leaf_dir.joinpath(entry)
#                 if full_path.is_file() and full_path.suffix.lower() in allowed_types:
#                     if check_file_needs_processing(full_path, session):
#                         photo_source_file = process_file(full_path, session, config, exif_tool_helper)
#                         uncomitted_photo_source_files.append(photo_source_file)
#                     else:
#                         logger.debug(f"Skipping unchanged file {full_path}")
#                 else:
#                     logging.debug(f"Skipping file {full_path} (unsupported type: {full_path.suffix})")
#         logger.info(f"Processed directory {leaf_dir}")
#         return uncomitted_photo_source_files
#     except Exception as e:
#         raise ValueError(f"Error processing directory {leaf_dir}: {e}")
#     finally:
#         session.close()


def select_reference_source(photo, preferences):
    """
    Given a Photo with multiple source files, select the reference source file
    based on the ordered preference list. For each preference, if a source file's
    absolute path contains the preference string, it is selected.
    If none match, return the first source file.
    """
    for pref in preferences:
        for psf in photo.source_files:
            if pref.lower() in psf.absolute_path_id.lower():
                return psf
    warnings.warn(f"No preferred reference source file found for photo {photo.timestamp_id}. Using first source file: {photo.source_files[0].absolute_path_id}.")
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
        ValueError(f"No source files found for photo {photo.timestamp_id}")
        return

    reference_file = select_reference_source(photo, source_file_preference)
    photo.reference_source_file = reference_file.absolute_path_id
    # Use reference file's label_color and rating.
    photo.label_color = reference_file.label_color
    photo.rating = reference_file.rating
    photo.keywords = reference_file.keywords
    # Merge EXIF data from all source files, making sure reference file's data takes precedence.
    photo.exif_metadata = merge_exif_data(photo.source_files, reference_file)
    return


from sqlalchemy import update


# def precompute_reference_data(session: Session, preferences):
#     """
#     Precompute reference source file data for each photo.
#     Returns a list of dicts with update values.
#     """
#     photos = session.query(Photo).options(joinedload(Photo.source_files)).all()
#     updates = []
#     for photo in photos:
#         reference_file = select_reference_source(photo, preferences)
#         exif_metadata = merge_exif_data(photo.source_files, reference_file)
#         updates.append({
#             "timestamp_id": photo.timestamp_id,
#             "reference_source_file": reference_file.absolute_path_id,
#             "label_color": reference_file.label_color,
#             "rating": reference_file.rating,
#             "exif_metadata": exif_metadata
#         })
#     return updates
#
#
# def update_photos(session: Session, config: Config):
#     preferences = config.source_file_preference
#
#     # Precompute updates in memory (still faster than individual commits)
#     updates = precompute_reference_data(session, preferences)
#     total_photos = len(updates)
#     logger.info(f"Precomputed updates for {total_photos} photos")
#
#     # Bulk update using SQLAlchemy Core
#     stmt = (
#         update(Photo)
#         .where(Photo.timestamp_id == bindparam("timestamp_id"))
#         .values(
#             reference_source_file=bindparam("reference_source_file"),
#             label_color=bindparam("label_color"),
#             rating=bindparam("rating"),
#             exif_metadata=bindparam("exif_metadata")
#         )
#     )
#     session.execute(stmt, updates)
#     session.commit()
#     logger.info(f"Updated {total_photos} photos in bulk")



def update_photos(session: Session, config: Config):
    """
    For each Photo in the database, retrieves its source files,
    selects a reference source file based on config preference order,
    merges EXIF data from source files, and updates the photo's label_color
    and rating.
    """
    preferences = config.source_file_preference
    #TODO: Only load photos with source files that have last_updtead > last_updated in photo. Requires adding last_updated to Photo
    photos = [photo for photo in session.query(Photo).options(joinedload(Photo.source_files)).all()]
    total_photos = len(photos)
    logger.info(f"Updating {total_photos} photos")
    for i, photo in enumerate(photos):
        update_photo(photo, preferences)
        if i % 1000 == 0:
            session.commit()
            #session.expunge_all()
            logger.info(f"Updated {i} of {total_photos} photos")
    logger.info(f"Updated {total_photos} photos")



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


def db_writer(db_objects_batch: List[List[Base]], config):
    """
    Write a batch of PhotoSourceFile objects to the database.
    """
    engine = create_engine(f'{config.db_url}?journal_mode=WAL',
                           connect_args={'check_same_thread': False},
                           poolclass=NullPool
                           )
    # Optionally, set WAL mode to improve write concurrency:
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA busy_timeout = 10000")

    session = Session(engine)
    try:
        for db_objects in db_objects_batch:
            for db_object in db_objects:
                session.merge(db_object)
        session.commit()
    except Exception as e:
        session.rollback()
        raise RuntimeError(f"Error writing to database: {e}. Object: {db_object}")
    finally:
        session.close()

class BatchProcessor:
    def __init__(self, batch_size: int, processing_fn: Callable[[List[Any]], None]):
        """
        Initialize the batch processor

        Args:
            batch_size: Maximum number of items to process in one batch
            processing_fn: Function that processes a batch of items
        """
        self.batch_size = batch_size
        self.processing_fn = processing_fn
        self.current_batch: List[Any] = []
        self.batches_processed = 0

    def process_item(self, item: Any) -> None:
        """Add item to current batch and process if batch is full"""
        self.current_batch.append(item)
        if len(self.current_batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Process current batch if not empty"""
        if self.current_batch:
            self.processing_fn(self.current_batch)
            self.batches_processed += len(self.current_batch)
            self.current_batch = []
            logger.info(f"Processed {self.batches_processed} batches, about {self.batches_processed * self.batch_size} items")




def consumer_process(queue: Queue, processor: BatchProcessor, stop_signal: str = "STOP"):
    """
    Consumer process that reads from queue and processes items in batches

    Args:
        queue: Queue to read items from
        batch_size: Maximum number of items to process in one batch
        stop_signal: Signal that indicates no more items will be produced
    """

    while True:
        try:
            # Wait for 1 second for new items
            item = queue.get(timeout=1)
            if item == stop_signal:
                break
            processor.process_item(item)
        except Empty:
            # If queue is empty, process any remaining items
            processor.flush()

    # Process any remaining items before shutting down
    processor.flush()

def main():
    # Load configuration
    config = Config.load_from_yaml("config.yaml")

    setup_root_stdout_root_logger()

    db_url = config.db_url

    # Create the engine and ensure tables exist.
    engine = create_engine(f'{db_url}?journal_mode=WAL', echo=False)
    Base.metadata.create_all(engine)
    logger.info("Database tables created.")
    print("If you see this and no logs, logging isn't being output")
    Session = sessionmaker(bind=engine)
    session = Session()
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA synchronous=NORMAL;")

    # Discover leaf directories from each parent directory in the config.
    files = []
    logger.info("Discovering files")
    for parent in config.original_photo_dirs:
        photo_files = list_files_recursive(parent, config.file_types)
        if len(photo_files) == 0:
            logger.warning(f"No files found in {parent}. Check that directory is mounted and config file_types are correct.")
        files.extend(photo_files)
    logger.info(f"Found {len(files)} files with correct type.")

    # Process files in batches
    batches = make_lazy_batches(files, config.batch_size)

    process_batch_partial = partial(process_batch, config=config)
    db_writer_partial = partial(db_writer, config=config)

    queue = Queue()

    # Start the consumer process
    processor = BatchProcessor(5, db_writer_partial)
    consumer = Process(target=consumer_process, args=(queue, processor))
    consumer.start()

    # Create a process pool for workers
    with Pool() as pool:
        # Start async processing of items
        for result in pool.imap_unordered(process_batch_partial, batches):
            queue.put(result)

    # Signal the consumer to stop
    queue.put("STOP")

    # Wait for consumer to finish
    consumer.join()

    # from cProfile import Profile
    #
    # from pstats import SortKey, Stats

    # Update photos after processing all directories.
    logger.info('Updating photos from photo_source_files')
    # Profile the update_photos function.
    # pr = Profile()
    # pr.enable()
    session.autoflush = False
    session.autocommit = False
    session.expire_on_commit = False
    update_photos(session, config)
    # pr.disable()

    # Print the profiling results.
    # s = io.StringIO()
    # ps = Stats(pr, stream=s).sort_stats('cumulative')
    # ps.print_stats()
    # print(s.getvalue())

    session.close()

def copy_photo_files_to_destination_directory(session, config: Config):
    """
    Retrieves Photos based on default filter in config, then copies the reference source files to config's destination directory.
    Preserves directory structure relative to original_photo_dirs.
    """

    #retrieve photos based on default filter


    #ensure destination directory exists
    dest_dir = Path(config.destination_directory)
    dest_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    main()

#TODO: 3/8/2025: works correctly. Need to clean up many functions and expand text coverage.