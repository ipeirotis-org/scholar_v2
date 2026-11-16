"""Tests for the query log's visitor fields: user agent, hashed IP, referrer, bot flag."""

from unittest import mock

import pytest

from frontend import visitor

CHROME = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/156.0.0.0 Safari/537.36")


@pytest.fixture(autouse=True)
def _salt(monkeypatch):
    monkeypatch.setattr(visitor, "_salt", None)
    monkeypatch.setattr(visitor, "_salt_checked_at", 0.0)
    monkeypatch.setenv("QUERY_LOG_SALT", "test-salt")


class TestBotUserAgent:
    @pytest.mark.parametrize("user_agent", [
        "",
        None,
        "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)",
        "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.4; +https://openai.com/gptbot)",
        "curl/8.5.0",
        "python-requests/2.32",
        "Go-http-client/1.1",
        "TLM-Audit-Scanner/2.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/124.0 Safari/537.36",
        "http://pip-score.org/wp-admin/install.php?step=1",
    ])
    def test_automated(self, user_agent):
        assert visitor.is_bot_user_agent(user_agent)

    @pytest.mark.parametrize("user_agent", [
        CHROME,
        "Mozilla/5.0 (iPhone; CPU iPhone OS 27_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "CriOS/157.0.8103.37 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.1.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0",
    ])
    def test_browsers(self, user_agent):
        assert not visitor.is_bot_user_agent(user_agent)


class TestIpHash:
    def test_stable_and_distinct(self):
        a, b = visitor.hash_ip("203.0.113.7"), visitor.hash_ip("203.0.113.8")
        assert a == visitor.hash_ip("203.0.113.7") and a != b
        assert len(a) == 16 and "203" not in a

    def test_depends_on_the_key(self, monkeypatch):
        first = visitor.hash_ip("203.0.113.7")
        monkeypatch.setattr(visitor, "_salt", None)
        monkeypatch.setenv("QUERY_LOG_SALT", "another-salt")
        assert visitor.hash_ip("203.0.113.7") != first

    def test_no_address_no_hash(self):
        assert visitor.hash_ip("") is None

    def test_no_key_no_hash(self, monkeypatch):
        """Without a secret key a hash could be reversed by trying every address: store nothing."""
        monkeypatch.delenv("QUERY_LOG_SALT")
        secretmanager = mock.MagicMock()
        secretmanager.SecretManagerServiceClient.side_effect = RuntimeError("permission denied")
        with mock.patch("google.cloud.secretmanager", secretmanager, create=True), \
             mock.patch.dict("sys.modules", {"google.cloud.secretmanager": secretmanager}):
            assert visitor.hash_ip("203.0.113.7") is None
            assert visitor.hash_ip("203.0.113.8") is None
        # The failed lookup is not repeated on every request.
        assert secretmanager.SecretManagerServiceClient.call_count == 1

    def test_key_from_secret_manager(self, monkeypatch):
        monkeypatch.delenv("QUERY_LOG_SALT")
        secretmanager = mock.MagicMock()
        access = secretmanager.SecretManagerServiceClient.return_value.access_secret_version
        access.return_value.payload.data = b"from-secret-manager\n"
        with mock.patch.dict("sys.modules", {"google.cloud.secretmanager": secretmanager}), \
             mock.patch("google.cloud.secretmanager", secretmanager, create=True):
            hashed = visitor.hash_ip("203.0.113.7")
        monkeypatch.setattr(visitor, "_salt", None)
        monkeypatch.setenv("QUERY_LOG_SALT", "from-secret-manager")
        assert hashed == visitor.hash_ip("203.0.113.7")


class TestDescribeVisitor:
    def _describe(self, app, **headers):
        with app.test_request_context("/get_similar_authors", headers=headers,
                                      base_url="https://www.pip-score.org",
                                      environ_base={"REMOTE_ADDR": "10.0.0.1"}):
            from flask import request
            return visitor.describe_visitor(request)

    def test_browser_from_another_site(self, app):
        info = self._describe(app, **{"User-Agent": CHROME, "X-Forwarded-For": "203.0.113.7, 10.1.2.3",
                                      "Referer": "https://www.Google.com/search?q=pip+score"})
        assert info == {"user_agent": CHROME, "ip_hash": visitor.hash_ip("203.0.113.7"),
                        "referrer": "www.google.com", "bot": False}

    def test_own_pages_are_not_a_referrer(self, app):
        info = self._describe(app, **{"User-Agent": CHROME, "Referer": "https://www.pip-score.org/index"})
        assert info["referrer"] == ""
        assert info["ip_hash"] == visitor.hash_ip("10.0.0.1")  # no proxy header: the socket peer

    def test_crawler_with_a_long_user_agent(self, app):
        info = self._describe(app, **{"User-Agent": "SomeBot/1.0 " + "x" * 500})
        assert info["bot"] is True and len(info["user_agent"]) == 300

    @pytest.mark.parametrize("referrer", ["http://[", "https://[::1", "http://a]b/"])
    def test_malformed_referrer_is_dropped(self, app, referrer):
        info = self._describe(app, **{"User-Agent": CHROME, "Referer": referrer})
        assert info["referrer"] == "" and info["bot"] is False
