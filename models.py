
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

class AutoTableNameMixin:
    @declared_attr
    def __tablename__(cls):
        return camel_to_snake(cls.__name__)

Base = declarative_base(cls=AutoTableNameMixin)


class PhotoSourceFile(Base):
    absolute_path_id = Column(String, primary_key=True)
    exif_metadata = Column(JSON)
    label_color = Column(String)
    rating = Column(Integer)
    timestamp = Column(String, ForeignKey('photo.timestamp_id'))
    photo = relationship('Photo', back_populates='source_files', foreign_keys=[timestamp])
    last_updated = Column(DateTime) #TODO: should I rename since this is the last time the file was updated?

class Photo(Base):
    timestamp_id = Column(String, primary_key=True)
    exif_metadata = Column(JSON)
    label_color = Column(String)
    rating = Column(Integer)
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


class PhotoFace(Base):
    id = Column(Integer, primary_key=True)
    location = Column(Integer, unique=True, nullable=False)
    sequence = Column(Integer, nullable=False)
    bb_x1 = Column(NUMERIC, nullable=False)
    bb_y1 = Column(NUMERIC, nullable=False)
    bb_x2 = Column(NUMERIC, nullable=False)
    bb_y2 = Column(NUMERIC, nullable=False)
    bb_prob = Column(NUMERIC, nullable=False)
    photo_ts_id = Column(DateTime, ForeignKey('photo.timestamp_id'))
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
        }
        return data

if __name__ == "__main__":
    from eralchemy import render_er
    render_er(Base, 'er.png')