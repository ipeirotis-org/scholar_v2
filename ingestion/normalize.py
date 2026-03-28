"""Shared document ID normalization.

Legacy data stored document IDs with a .json suffix (e.g., "abc123.json").
Newer data omits the suffix. All code paths must normalize to the bare ID.
"""


def normalize_document_id(doc_id):
    """Strip trailing .json suffix from a document ID.

    >>> normalize_document_id("abc123.json")
    'abc123'
    >>> normalize_document_id("abc123")
    'abc123'
    >>> normalize_document_id("scholar_id:pub_id.json")
    'scholar_id:pub_id'
    """
    if doc_id and doc_id.endswith(".json"):
        return doc_id[:-5]
    return doc_id
