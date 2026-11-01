"""Load vendor keys from Secret Manager into memory; never print them."""
import os, subprocess
_ENV = {k: v for k, v in os.environ.items() if k != "CLOUDSDK_AUTH_ACCESS_TOKEN"}
NAMES = {"serpapi": "serpapi-key", "searchapi": "searchapi-key", "scraperapi": "scraper-api-key"}
KEYS = {}
def load():
    for k, s in NAMES.items():
        KEYS[k] = subprocess.run(["gcloud", "secrets", "versions", "access", "latest", f"--secret={s}",
                                  "--project=scholar-version2"], capture_output=True, text=True, env=_ENV,
                                 check=True).stdout.strip()
    return KEYS
def scrub(text):
    for v in KEYS.values():
        if v:
            text = str(text).replace(v, "***")
    return text
