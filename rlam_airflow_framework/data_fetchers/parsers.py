# File: rlam_airflow_framework/data_fetchers/parsers.py
"""
Format parsers shared by every fetcher.

Both HttpFetcher and SftpFetcher reduce their payload to a text string and
hand it to ``parse_content()``, so the json/csv/xml parsing logic (and its
error handling) lives in exactly one place instead of being duplicated per
transport.
"""

import json
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List, Callable
import polars as pl
import structlog

log = structlog.get_logger(__name__)


def _xml_root_to_records(root: ET.Element) -> List[Dict[str, Any]]:
    """
    Flatten an XML root into a list of records (one dict per direct child).

    Each grandchild becomes a key/value (tag -> text). Empty children are skipped.
    """
    records: List[Dict[str, Any]] = []
    for child in root:
        item: Dict[str, Any] = {}
        for subchild in child:
            item[subchild.tag] = subchild.text
        if item:  # Only add non-empty items
            records.append(item)
    return records


def parse_json(content: str) -> pl.DataFrame:
    if not content or content.isspace():
        log.warning("JSON content is empty")
        return pl.DataFrame()

    data = json.loads(content)
    if data is None:
        log.warning("JSON content is null")
        return pl.DataFrame()
    if isinstance(data, list) and len(data) == 0:
        log.warning("JSON content is empty array")
        return pl.DataFrame()
    return pl.DataFrame(data)


def parse_csv(content: str) -> pl.DataFrame:
    if not content or content.isspace():
        log.warning("CSV content is empty")
        return pl.DataFrame()
    return pl.read_csv(content.encode("utf-8"))


def parse_xml(content: str) -> pl.DataFrame:
    if not content or content.isspace():
        log.warning("XML content is empty")
        return pl.DataFrame()

    data = _xml_root_to_records(ET.fromstring(content))
    if not data:
        log.warning("No data extracted from XML")
    return pl.DataFrame(data)


PARSERS: Dict[str, Callable[[str], pl.DataFrame]] = {
    "json": parse_json,
    "csv": parse_csv,
    "xml": parse_xml,
}


def parse_content(
    content: str, format: str, trace_id: Optional[str] = None
) -> pl.DataFrame:
    """
    Parse text content according to ``format`` ('json', 'csv', or 'xml').

    Raises:
        ValueError: If format is unsupported or the content fails to parse.
    """
    parser = PARSERS.get(format)
    if parser is None:
        raise ValueError(f"Unsupported format: {format}. Supported: json, csv, xml")

    bound_log = log.bind(trace_id=trace_id) if trace_id else log
    try:
        return parser(content)

    except json.JSONDecodeError as e:
        bound_log.error(f"Failed to parse JSON content: {e}")
        raise ValueError(f"Invalid JSON response: {e}") from e

    except pl.exceptions.NoDataError as e:
        bound_log.warning(f"Empty data in content: {e}")
        return pl.DataFrame()

    except ET.ParseError as e:
        bound_log.error(f"Failed to parse XML content: {e}")
        raise ValueError(f"Invalid XML response: {e}") from e
