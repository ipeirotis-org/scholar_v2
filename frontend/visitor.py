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

# User agents that declare automation, the empty one, and any that does not start like a
# browser's (every browser sends "Mozilla/5.0 ..."; 2026-11-19: "panscient.com" opened 112
# profiles in 30 minutes). Scrapers that send a browser's user agent are not caught here; the
# hashed IP lets them be grouped by behaviour afterwards (many profiles within a minute, no
# search activity), and the homepage's recent list asks for a view token instead (below).
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


_BROWSER_UA_PREFIXES = ("Mozilla/", "Opera/")

# A profile page counts as read by a person once it has been open and visible this long.
VIEW_MIN_SECONDS = 5
# A view token is accepted for this long after the page was served.
VIEW_MAX_SECONDS = 3600


def is_bot_user_agent(user_agent):
    return (not user_agent or not user_agent.startswith(_BROWSER_UA_PREFIXES)
            or bool(_BOT_UA_RE.search(user_agent)))


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


def _view_signature(salt, author_id, issued):
    return hmac.new(salt, f"view:{author_id}:{issued}".encode("utf-8"), hashlib.sha256).hexdigest()[:32]


def view_token(author_id, now=None):
    """A signed "<issued>.<signature>" token for one profile page, or None without the key.

    The results page hands it to the browser, which posts it back once the page has been
    visible for VIEW_MIN_SECONDS (/api/recent_author). Only then does the author enter the
    homepage's recent list. A crawler that fetches the HTML never posts it; one that
    drives a browser has to keep each page open, and the server checks the age itself.
    Signed with the query-log key, which every instance shares (SECRET_KEY is per instance).
    """
    salt = _get_salt()
    if not salt or not author_id:
        return None
    issued = int(time.time() if now is None else now)
    return f"{issued}.{_view_signature(salt, author_id, issued)}"


def view_token_age(author_id, token, now=None):
    """Seconds since `token` was issued for this author; None if it is not one of ours."""
    salt = _get_salt()
    issued, _, signature = (token or "").partition(".")
    if not salt or not author_id or not issued.isdigit() or len(issued) > 12:
        return None
    if not hmac.compare_digest(signature, _view_signature(salt, author_id, int(issued))):
        return None
    return (time.time() if now is None else now) - int(issued)


def is_read_by_person(author_id, token, now=None):
    """True when the token is valid and the page has been open between the two limits."""
    age = view_token_age(author_id, token, now)
    return age is not None and VIEW_MIN_SECONDS <= age <= VIEW_MAX_SECONDS


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
