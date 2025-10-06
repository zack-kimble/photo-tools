from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

from sqlalchemy import and_, or_, extract, cast, String, func, column
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.orm import Query
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select

from models import Photo
from config import PhotoFilterConfig


def _date_part_expr(timestamp_col, date_parts: Dict[str, Any]):
    """
    Build AND of equality expressions for provided date parts.
    """
    clauses: List[BinaryExpression] = []
    # Use extract/strftime depending on dialect
    for part, value in date_parts.items():
        clauses.append(extract(part, timestamp_col) == int(value))

    return clauses


def _keywords_or_clause(keywords: Iterable[str]):
    """
    Build an OR clause that matches any of the given keywords in a JSON list column.

    - For PostgreSQL: use JSONB containment (col @> '["kw"]').
    - Fallback (SQLite/others): string search on the JSON text for `"kw"` token (quoted),
      which avoids most substring false-positives.
    """
    # todo: lower both keywords and column values for case insensitive match
    keywords = [kw.lower() for kw in keywords]
    json_table = func.json_each(Photo.keywords).table_valued('value')
    clause = func.lower(json_table.c.value).in_(keywords)
    return clause


def build_photo_filter_clauses(
    filter: PhotoFilterConfig,
) -> List[BinaryExpression]:
    """
    Translate a filter dict (from your YAML) into a list of SQLAlchemy clauses
    (combine them with AND in your query).

    Supported keys:
      - start_date (YYYY-MM-DD or any ISO string comparable to timestamp_id)
      - end_date
      - date_part: {year, month, day, hour, minute, second}
      - keywords: [str, ...]  (OR)
      - labels: [str, ...]    (OR)
      - rating: int  (>=)

    Args:
      f: filter dictionary.
      dialect_name: session.bind.dialect.name (e.g., "sqlite", "postgresql", "mysql").
      timestamp_is_datetime: set True if Photo.timestamp_id is a Date/DateTime column.

    Returns:
      List of SQLAlchemy boolean expressions.
    """
    clauses: List[BinaryExpression] = []

    # Date range: string-compare on ISO strings OR datetime compare if set
    if start := filter.start_date:
        clauses.append(Photo.timestamp_id >= start)
    if end := filter.end_date:
        clauses.append(Photo.timestamp_id <= end)

    # date_part
    if dp := filter.date_parts:
        clauses.extend(
            _date_part_expr(
                Photo.timestamp_id,
                dp,
            )
        )

    # keywords (OR)
    if filter.keywords:
        kw_clause = _keywords_or_clause(filter.keywords)
        clauses.append(kw_clause)

    # labels (OR)
    if filter.labels:
        clauses.append(Photo.label_color.in_(filter.labels))

    # rating (>=)
    if rating := filter.rating:
        clauses.append(Photo.rating >= int(rating))

    return clauses

#Todo: make filter pydantic model and validate datetimes and other structures
def apply_photo_filter(
    query_or_select: Query | Select,
    filter: PhotoFilterConfig,
    *,
    dialect_name: str | None = None,
):
    """
    Given a Query/Select on Photo and a filter dict, apply the WHERE conditions and return the filtered query.

    Example:
        filt = {
            "start_date": "2024-05-01",
            "end_date": "2025-07-10",
        }
        q = session.query(Photo)
        q = apply_photo_filter(q, filt, dialect_name=session.bind.dialect.name)
        rows = q.all()
    """
    clauses = build_photo_filter_clauses(
        filter,
    )
    if not clauses:
        return query_or_select
    return query_or_select.filter(and_(*clauses))
