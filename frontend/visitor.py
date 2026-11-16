"""Who made a request, for the query log: user agent, hashed IP, referrer, bot flag.

The Cloud Run request logs hold the same information but only for 30 days. The
query log keeps it so searches and profile views by people can be told from
crawler traffic later. The IP address itself is never stored: it is replaced by
a keyed hash (HMAC-SHA256), stable enough to group one visitor's requests.
"""

import hashlib
import hmac
import logging
import os
import re
import threading
import time
from urllib.parse import urlsplit

from frontend.config import Config

logger = logging.getLogger(__name__)

# User agents that declare automation, plus the empty one. Scrapers that send a
# browser's user agent are not caught here; the hashed IP lets them be grouped
# by behaviour afterwards (many profiles within a minute, no search activity).
_BOT_UA_RE = re.compile(
    r"bot|crawl|spider|scan|slurp|python|curl|wget|go-http|okhttp|headless|httpclient|java/|libwww|axios"
    r"|node-fetch|scrapy|monitor|preview|facebookexternalhit|lighthouse|phantom|feed|pdfmin|audit|https?://",
    re.IGNORECASE,
)

_USER_AGENT_MAX = 300
_SALT_RETRY_SECONDS = 600

_salt = None
_salt_checked_at = 0.0
_salt_lock = threading.Lock()


def is_bot_user_agent(user_agent):
    return not user_agent or bool(_BOT_UA_RE.search(user_agent))


def client_ip(request):
    """The visitor's address: the first X-Forwarded-For entry on Cloud Run, else the socket peer."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    return forwarded.split(",")[0].strip() or request.remote_addr or ""


def _get_salt():
    """The secret key for IP hashes, from QUERY_LOG_SALT or Secret Manager; None if unavailable.

    Without a secret key the hashes could be reversed by trying every address,
    so no hash is stored until one is configured. A failed lookup is retried
    every ten minutes.
    """
    global _salt, _salt_checked_at
    if _salt:
        return _salt
    with _salt_lock:
        if _salt:
            return _salt
        value = os.environ.get("QUERY_LOG_SALT", "").strip()
        if value:
            _salt = value.encode("utf-8")
            return _salt
        if time.time() - _salt_checked_at < _SALT_RETRY_SECONDS:
            return None
        _salt_checked_at = time.time()
        try:
            from google.cloud import secretmanager
            client = secretmanager.SecretManagerServiceClient()
            response = client.access_secret_version(name=Config.QUERY_LOG_SALT_SECRET)
            payload = response.payload.data
            value = payload.decode("utf-8").strip() if isinstance(payload, bytes) else ""
            _salt = value.encode("utf-8") if value else None
        except Exception:
            logger.warning("Query log salt unavailable; IP hashes are not being stored")
            _salt = None
        return _salt


def hash_ip(ip):
    """A 16-hex-digit keyed hash of the address, or None without an address or key."""
    salt = _get_salt()
    if not ip or not salt:
        return None
    return hmac.new(salt, ip.encode("utf-8"), hashlib.sha256).hexdigest()[:16]


def describe_visitor(request):
    """Query-log fields for the request's sender. Call it in the request thread."""
    user_agent = request.headers.get("User-Agent", "")
    referrer = request.headers.get("Referer", "")
    # Only other sites are worth keeping: they say how the visitor got here.
    try:
        referrer_host = urlsplit(referrer).netloc.lower() if referrer else ""
    except ValueError:
        # A malformed header ("http://[") must not fail the request it came with.
        referrer_host = ""
    if referrer_host == request.host.lower():
        referrer_host = ""
    return {
        "user_agent": user_agent[:_USER_AGENT_MAX],
        "ip_hash": hash_ip(client_ip(request)),
        "referrer": referrer_host[:100],
        "bot": is_bot_user_agent(user_agent),
    }
