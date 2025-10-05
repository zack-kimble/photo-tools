from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

from sqlalchemy import and_, or_, extract, cast, String, func, column
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.orm import Query
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select


from models import Photo



def _date_part_expr(timestamp_col, date_parts: Dict[str, Any], dialect_name: str):
    """
    Build AND of equality expressions for provided date parts.
    """
    clauses: List[BinaryExpression] = []
    # Use extract/strftime depending on dialect
    for part, value in date_parts.items():
        clauses.append(extract(part, timestamp_col) == int(value))

    return clauses


def _keywords_or_clause(dialect_name: str, keywords: Iterable[str], json_col):
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
    f: Dict[str, Any],
    *,
    dialect_name: str,
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
    if start := f.get("start_date"):
        clauses.append(Photo.timestamp_id >= start)
    if end := f.get("end_date"):
        clauses.append(Photo.timestamp_id <= end)

    # date_part
    if dp := f.get("date_part"):
        clauses.extend(
            _date_part_expr(
                Photo.timestamp_id,
                dp,
                dialect_name=dialect_name
            )
        )

    # keywords (OR)
    if "keywords" in f and f["keywords"] not in (None, ""):
        #todo: make sure keyword is a list in Pydantic model and remove this check
        kw_val = f["keywords"]
        if isinstance(kw_val, str):
            kw_list = [kw_val]
        else:
            kw_list = list(kw_val)
        kw_clause = _keywords_or_clause(dialect_name, kw_list, Photo.keywords)
        if kw_clause is not None:
            clauses.append(kw_clause)

    # labels (OR)
    if "labels" in f and f["labels"] not in (None, ""):
        labels_val = f["labels"]
        if isinstance(labels_val, str):
            labels_list = [labels_val]
        else:
            labels_list = list(labels_val)
        clauses.append(Photo.label_color.in_(labels_list))

    # rating (>=)
    if rating := f.get("rating"):
        clauses.append(Photo.rating >= int(rating))

    return clauses

#Todo: make filter pydantic model and validate datetimes and other structures
def apply_photo_filter(
    query_or_select: Query | Select,
    filter_dict: Dict[str, Any],
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
    if dialect_name is None:
        # Try to sniff from the query if possible (best effort)
        try:
            dialect_name = query_or_select.session.bind.dialect.name  # type: ignore[attr-defined]
        except AttributeError:
            # Fallback to a generic choice
            dialect_name = "sqlite"

    clauses = build_photo_filter_clauses(
        filter_dict,
        dialect_name=dialect_name,
    )
    if not clauses:
        return query_or_select
    return query_or_select.filter(and_(*clauses))
