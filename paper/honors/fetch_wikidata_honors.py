"""Fetch scientific honors from Wikidata for people linked to a Semantic Scholar author ID, and load them to BigQuery.

1. Every Wikidata item with a Semantic Scholar author ID (P4012)          -> data/s2_items.csv
2. Their awards (P166, year from P585) and memberships (P463, year from P580)
3. Keep curated honors for research achievement (HONORS below):
     senior  national academies, learned-society fellowships and major prizes
     early   early-career awards (Sloan, PECASE, Packard, CNRS bronze medal), kept apart for timing reasons
   Excluded on purpose: plain society memberships, state orders, student scholarships, honorary doctorates, and
   citation-based honors (Clarivate Citation Laureates), which would make the validation circular.
                                                                         -> data/honors_curated.csv
4. Google Scholar route: for each senior honor, the honored people whose Wikidata item carries a Google Scholar ID
   (P1960), with the year when given (one query per honor; a single query times out)  -> data/gs_honors_dated.csv
5. Load into `scholar-version2.paper_rp.wikidata_s2_items`, `.wikidata_honors` and `.wikidata_gs_honors` (needs `bq`).

Run: python3 paper/honors/fetch_wikidata_honors.py   (fetched 2026-11-05: 41,272 linked items, 4,222 honor rows)
"""
import csv
import io
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "pip-score-research/1.0 (https://www.pip-score.org)"

SENIOR = """Q5442484 Q463303 Q52382875 Q270794 Q337234 Q123885 Q15631401 Q14906020 Q543804 Q723551 Q27988727 Q24081923
Q18748039 Q466089 Q94301 Q3305342 Q111734 Q41955909 Q26268243 Q1376791 Q4671531 Q1493021 Q83172 Q338432 Q253439
Q61744587 Q5438598 Q117467 Q188771 Q61663688 Q18748042 Q61744980 Q4352382 Q530471 Q30636749 Q299015 Q59771847
Q117311892 Q59766840 Q59260322 Q59772620 Q371160 Q59767712 Q110587 Q191583 Q265058 Q684415 Q219989 Q414188 Q1202021
Q337526 Q337352 Q61679753 Q1316544 Q737051 Q1543268 Q1031994 Q703205 Q5019489 Q6085042 Q18357422 Q873412 Q3332287
Q1630682 Q8010631""".split()
EARLY = "Q7541057 Q7241433 Q113469875 Q3332320".split()
HONORS = {**{q: "senior" for q in SENIOR}, **{q: "early" for q in EARLY}}


def sparql(query, accept="text/csv"):
    url = SPARQL + "?" + urllib.parse.urlencode({"query": query})
    req = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as r:
        return r.read().decode()


def qid(uri):
    return uri.rsplit("/", 1)[1]


def main():
    os.makedirs(DATA, exist_ok=True)
    items = list(csv.DictReader(io.StringIO(sparql("SELECT ?p ?s2 WHERE { ?p wdt:P4012 ?s2 }"))))
    statements = list(csv.DictReader(io.StringIO(sparql("""SELECT ?p ?s2 ?prop ?honor ?date WHERE {
      ?p wdt:P4012 ?s2 .
      { ?p p:P166 ?st . ?st ps:P166 ?honor . OPTIONAL { ?st pq:P585 ?date } BIND("award" AS ?prop) }
      UNION
      { ?p p:P463 ?st . ?st ps:P463 ?honor . OPTIONAL { ?st pq:P580 ?date } BIND("member" AS ?prop) }
    }"""))))
    values = " ".join("wd:" + q for q in HONORS)
    labels = {qid(b["h"]["value"]): b["l"]["value"] for b in json.loads(sparql(
        f'SELECT ?h ?l WHERE {{ VALUES ?h {{ {values} }} ?h rdfs:label ?l FILTER(LANG(?l) = "en") }}',
        accept="application/sparql-results+json"))["results"]["bindings"]}
    with open(os.path.join(DATA, "s2_items.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["wikidata_id", "s2_authorid"])
        for r in items:
            w.writerow([qid(r["p"]), r["s2"]])
    n = 0
    with open(os.path.join(DATA, "honors_curated.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["wikidata_id", "s2_authorid", "honor_qid", "honor", "category", "honor_year"])
        for r in statements:
            q = qid(r["honor"])
            if q not in HONORS:
                continue
            m = re.match(r"^(\d{4})-", r["date"])  # "unknown value" dates come back as blank-node URIs
            w.writerow([qid(r["p"]), r["s2"], q, labels.get(q, q), HONORS[q], m.group(1) if m else ""])
            n += 1
    print(f"{len(items)} linked items, {n} curated honor rows")
    gs_rows = set()
    for q in SENIOR:
        for prop, qualifier in (("P166", "P585"), ("P463", "P580")):  # separate queries: a UNION times out
            txt = sparql(f"SELECT ?p ?gs ?date WHERE {{ ?p p:{prop} ?st . ?st ps:{prop} wd:{q} . "
                         f"?p wdt:P1960 ?gs . OPTIONAL {{ ?st pq:{qualifier} ?date }} }}")
            if "Exception" in txt:
                raise RuntimeError(f"Wikidata query for {q} / {prop} timed out; rerun")
            for r in csv.DictReader(io.StringIO(txt)):
                m = re.match(r"^(\d{4})-", r["date"] or "")
                gs_rows.add((qid(r["p"]), r["gs"], q, m.group(1) if m else ""))
    with open(os.path.join(DATA, "gs_honors_dated.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["wikidata_id", "scholar_id", "honor_qid", "honor_year"])
        w.writerows(sorted(gs_rows))
    print(f"{len({r[0] for r in gs_rows})} honored people with a Google Scholar ID")
    env = {k: v for k, v in os.environ.items() if k != "CLOUDSDK_AUTH_ACCESS_TOKEN"}
    for table, path, schema in (
        ("wikidata_s2_items", "s2_items.csv", "wikidata_id:STRING,s2_authorid:STRING"),
        ("wikidata_honors", "honors_curated.csv",
         "wikidata_id:STRING,s2_authorid:STRING,honor_qid:STRING,honor:STRING,category:STRING,honor_year:INTEGER"),
        ("wikidata_gs_honors", "gs_honors_dated.csv",
         "wikidata_id:STRING,scholar_id:STRING,honor_qid:STRING,honor_year:INTEGER"),
    ):
        subprocess.run(["bq", "load", "--replace", "--source_format=CSV", "--skip_leading_rows=1",
                        f"scholar-version2:paper_rp.{table}", os.path.join(DATA, path), schema], check=True, env=env)


if __name__ == "__main__":
    main()
