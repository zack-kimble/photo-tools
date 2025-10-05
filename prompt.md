Given the following config, ORM models and pseudocode, create a script executes the pseudocode using the ORM objects. Please use a clean design with little duplication of code and functions that follow SOLID. Only create new classes if necessary. Create a logger and log any file specific errors or missing values.
config:
```yaml
original_photo_dirs : ["/media/zack/WD 4TB/My Pictures/D800/", "/media/zack/WD 4TB/My Pictures/D700/Originals/", "/media/zack/WD 4TB/My Pictures/Z7/"],
dt_keys : ["Composite:SubSecCreateDate", "Composite:CreateDate", "XMP:DateTimeOriginal"],
file_types : ["jpg", "nef", "tiff", "xmp", "jpeg"],
source_file_preference:
  - lr_edited_jpgs
  - /media/zack/WD 4TB/My Pictures/Z7/
  - lr_jpgs/My Pictures/D800
  - /media/zack/WD 4TB/My Pictures/D700/picasa
  - /media/zack/WD 4TB/My Pictures/D700
  - lr_jpgs
```

ORM:
```python

import re

# from app.search import add_to_index, remove_from_index, query_index
from sqlalchemy import types, Column, Integer, String, ForeignKey, Boolean, JSON, DateTime, NUMERIC
from sqlalchemy.orm import relationship
import numpy as np
import io

from sqlalchemy.ext.declarative import declarative_base, declared_attr


def camel_to_snake(name):
    # Convert CamelCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

class AutoTableNameMixin:
    @declared_attr
    def __tablename__(cls):
        return camel_to_snake(cls.__name__)

Base = declarative_base(cls=AutoTableNameMixin)


class ArrayType(types.TypeDecorator):
    impl = types.BLOB

    # def __repr__(self):
    #     return self.impl.__repr__()

    def process_bind_param(self, value, dialect):
        out = io.BytesIO()
        np.save(out, value)
        out.seek(0)
        return out.read()

    def process_result_value(self, value, dialect):
        out = io.BytesIO(value)
        out.seek(0)
        return np.load(out)

class PhotoSourceFile(Base):
    absolute_path = Column(String, primary_key=True)
    exif_metadata = Column(JSON)
    label_color = Column(String)
    rating = Column(Integer)
    timestamp = Column(DateTime, ForeignKey('photo.timestamp_id'))

class Photo(Base):
    timestamp_id = Column(DateTime, primary_key=True)
    exif_metadata = Column(JSON)
    label_color = Column(String)
    rating = Column(Integer)
    photo_faces = relationship('PhotoFace')
    search_results = relationship('SearchResults', back_populates='photo')
    face_detection_run = Column(Boolean, nullable=False, default=False)
    source_files = relationship('PhotoSourceFile', back_populates='photo')
    reference_source_file = Column(String, ForeignKey('photo_source_file.absolute_path'), unique=True)

    def to_dict(self):
        data = {
            'photo_id': self.id,
            'location': self.location,
            'metadata': self.photo_metadata,
        }
        return data


class PhotoFace(Base):
    id = Column(Integer, primary_key=True)
    location = Column(Integer, unique=True, nullable=False)
    sequence = Column(Integer, nullable=False)
    bb_x1 = Column(NUMERIC, nullable=False)
    bb_y1 = Column(NUMERIC, nullable=False)
    bb_x2 = Column(NUMERIC, nullable=False)
    bb_y2 = Column(NUMERIC, nullable=False)
    bb_prob = Column(NUMERIC, nullable=False)
    photo_ts_id = Column(Integer, ForeignKey('photo.timestamp_id'))
    photo = relationship("Photo", back_populates='photo_faces')
    bb_auto = Column(Boolean)
    name = Column(String)
    name_auto = Column(Boolean)
    embedding = relationship('FaceEmbedding')

    def from_dict(self, data):
        for field in data:
            setattr(self, field, data[field])

class FaceEmbedding(Base):
    id = Column(Integer, primary_key=True)
    # embedding = Column(JSON)
    embedding = Column(ArrayType)
    photo_face_id = Column(Integer, ForeignKey('photo_face.id'), unique=True)


class SavedSearch(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    people = Column(String)
    keywords = Column(String)

class SearchResults(Base):
    id = Column(Integer, primary_key=True)
    search_id = Column(Integer, ForeignKey('saved_search.id'))
    photo_timestamp_id = Column(DateTime, ForeignKey('photo.timestamp_id'))
    #TODO: change this to "result_index" or "search_index"
    order_by = Column(Integer, index=True)
    photo = relationship('Photo', back_populates='search_results')

    def to_dict(self):
        data = {
            'search_result_id': self.id,
            'search_id': self.search_id,
            'photo_id': self.photo_timestamp_id,
            'order_by': self.order_by,
            'location': self.photo.location,
        }
        return data

if __name__ == "__main__":
    from eralchemy import render_er
    render_er(Base, 'er.png')
```


and the following pseudocode:
```
Load configs
get all leaf directories for each parent source directory in configs
create tasks in multiproces queue for each leaf directory
for each task
    get all files in leaf directory
    for each file
        extract exif data using Pillow
        attempt to normalize key exif data (datetime, rating, label color). For each use the first non-null value from the keys specified in the config.
        create a photo_source_file object and write to database
        create a photo object if it doesn't exist and write to database (ust ts_id right now)
for each photo object in db
    retrieve all source file objects
    determine reference source file based on source_file_preference provided in config. The filepath of the reference source file that contains the lowest ranked member in the list should be chosen.
    add to photo object
    merge exif data from source file objects and add to photo object
    merge label_color and rating from source file objects and add to photo object (if not present in reference source file, use first non-null value with same prefernce order)
    update photo object in db

```



Create a python function that accepts a filter defined in yaml and translates it into a sqlalchemy query filter. Assume that the attributes of a filter are all connected by "and" statements and that any values given as lists are connect with "or" statements. The filter can contain the following attributes: start_date, end_date, date_part (which can contain datetime parts), keywords (list), labels (list), rating (int). `start_date` filters to pictures with `Photo.timestamp_id >= start_date`, `end_date` filters to pictures with `Photo.timestamp_id <= end_date`. `date_part` can contain any datetime.datetime date parts and filters to pictures where the corresponding part of `Photo.timestamp_id` matches the value. `keywords` filters to pictures where any of the keywords are present in `Photo.keywords` (which is a list). `labels` filters to pictures where `Photo.label_color` is in the list of labels. `rating` filters to pictures where `Photo.rating >= rating`.

Example filters:

```yaml
default_filter:
  labels: "Red"
slideshows:
  - name: "Last Year"
    description: "photos from the last year"
    filter:
      start_date: "2024-05-01"
      end_date: "2025-07-10"

  - name: "Asia 2014"
    description: "photos from my Asia trip in 2014"
    filter:
      keywords: ["Asia 2014"]

  - name: "Mountains"
    description: "mountain photos"
    filter:
      keywords: ["mountain", "mountains"]

  - name: "Christmas"
    description: "Christmas photos"
    filter:
      date_part:
        month: 12
  
  - name: "best"
    description: "best photos"
    filter:
      rating: 5
```

Photo model:
```python
class Photo(Base):
    timestamp_id = Column(String, primary_key=True)
    exif_metadata = Column(JSON)
    label_color = Column(String)
    rating = Column(Integer)
    keywords: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(JSON),   # track in-place .append/.remove changes
        default=list,                   # avoid default=[]
        nullable=False
    )
    photo_faces = relationship('PhotoFace')
    search_results = relationship('SearchResults', back_populates='photo')
    face_detection_run = Column(Boolean, nullable=False, default=False)
    source_files = relationship('PhotoSourceFile', back_populates='photo', foreign_keys=[PhotoSourceFile.timestamp])
    reference_source_file = Column(String, ForeignKey('photo_source_file.absolute_path_id'), unique=True)
    reference_source = relationship(
        'PhotoSourceFile',
        foreign_keys=[reference_source_file]
    )

    def to_dict(self):
        return {
            'photo_id': self.timestamp_id,
            'metadata': self.exif_metadata,
            'label_color': self.label_color,
            'rating': self.rating,
        }
        return data
```



given the following filters and photo model below, create test photo objects. There should be at least 1 object returned for each query and multiple that are not.

```yaml
  - name: "Last Year"
    description: "photos from the last year"
    filter:
      start_date: "2024-05-01"
      end_date: "2025-07-10"

  - name: "Asia 2014"
    description: "photos from my Asia trip in 2014"
    filter:
      keywords: ["Asia 2014"]

  - name: "Mountains"
    description: "mountain photos"
    filter:
      keywords: ["mountain", "mountains"]

  - name: "Christmas"
    description: "Christmas photos"
    filter:
      date_part:
        month: 12

  - name: "best"
    description: "best photos"
    filter:
      rating: 5
```

```python
class Photo(Base):
    timestamp_id = Column(DateTime, primary_key=True)
    exif_metadata = Column(JSON)
    label_color = Column(String)
    rating = Column(Integer)
    keywords: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(JSON),   # track in-place .append/.remove changes
        default=list,                   # avoid default=[]
        nullable=False
    )
    photo_faces = relationship('PhotoFace')
    search_results = relationship('SearchResults', back_populates='photo')
    face_detection_run = Column(Boolean, nullable=False, default=False)
    source_files = relationship('PhotoSourceFile', back_populates='photo', foreign_keys=[PhotoSourceFile.timestamp])
    reference_source_file = Column(String, ForeignKey('photo_source_file.absolute_path_id'), unique=True)
    reference_source = relationship(
        'PhotoSourceFile',
        foreign_keys=[reference_source_file]
    )

    def to_dict(self):
        return {
            'photo_id': self.timestamp_id,
            'metadata': self.exif_metadata,
            'label_color': self.label_color,
            'rating': self.rating,
        }
        return data

```