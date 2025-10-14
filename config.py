from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict
from pathlib import Path

from ruamel.yaml import YAML
safe_yaml = YAML(typ="safe")

class PossibleMetadataKeys(BaseModel):
    timestamp: List[str]
    label_color: List[str]
    rating: List[str]


class PhotoFilterConfig(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_parts: Optional[Dict[str, int]] = None
    keywords: Optional[List[str]] = None
    labels: Optional[List[str]] = None
    rating: Optional[int] = None

    @field_validator('rating')
    @classmethod
    def validate_rating(cls, v):
        if v is not None and not 1 <= v <= 5:
            raise ValueError('Rating must be between 1 and 5')
        return v

    def merge(self, other: "PhotoFilterConfig") -> "PhotoFilterConfig":
        """
        Merge another PhotoFilterConfig into this one, with `other` taking precedence.
        Lists are combined (union), scalars from `other` override if set.
        """
        merged = self.model_copy()

        if other.start_date is not None:
            merged.start_date = other.start_date
        if other.end_date is not None:
            merged.end_date = other.end_date
        if other.date_parts is not None:
            merged.date_parts = {**(self.date_parts or {}), **other.date_parts}
        if other.keywords is not None:
            merged.keywords = list(set((self.keywords or []) + other.keywords))
        if other.labels is not None:
            merged.labels = list(set((self.labels or []) + other.labels))
        if other.rating is not None:
            merged.rating = other.rating

        return merged


class SlideShowConfig(BaseModel):
    name: str
    description: str
    filter: PhotoFilterConfig  # Automatic nested validation!



class Config(BaseModel):
    original_photo_dirs: List[Path]
    file_types: List[str]
    possible_metadata_keys: PossibleMetadataKeys
    source_file_preference: List[str]
    destination_dir: Path
    db_url: str = "sqlite:///photos.db"
    batch_size: int = 100
    default_filter: PhotoFilterConfig = Field(default_factory=PhotoFilterConfig)
    slideshows: Optional[List[SlideShowConfig]] = None

    @classmethod
    def load_from_yaml(cls, yaml_path: str) -> "Config":
        with open(yaml_path, "r") as f:
            data = safe_yaml.load(f)
        return cls.model_validate(data)

