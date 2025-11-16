import os
import time
import sys
import argparse
import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from datetime import datetime
from math import ceil
load_dotenv()

GQL_URL = "https://api.github.com/graphql"
# GraphQL search returns up to 100 nodes per request
PAGE_SIZE = 100
# max retries on transient HTTP errors
MAX_RETRIES = 6

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")

if not GITHUB_TOKEN:
    print("GITHUB_TOKEN is required in env. In Actions this is provided by default.", file=sys.stderr)
    sys.exit(1)

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

# GraphQL query: search repositories, return id, nameWithOwner, url, stargazerCount
GQL_QUERY = """
query ($q: String!, $first: Int!, $after: String) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  search(query: $q, type: REPOSITORY, first: $first, after: $after) {
    repositoryCount
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        id
        nameWithOwner
        url
        stargazerCount
      }
    }
  }
}
"""

def graphql_request(variables):
    backoff = 1.0
    for attempt in range(1, MAX_RETRIES+1):
        resp = requests.post(GQL_URL, json={"query": GQL_QUERY, "variables": variables}, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            j = resp.json()
            if "errors" in j:
                # sometimes GitHub returns rate-limit or other transient messages in errors
                print("GraphQL returned errors:", j["errors"], file=sys.stderr)
                # treat as transient
                time.sleep(backoff)
                backoff *= 2
                continue
            return j
        elif resp.status_code in (502, 503, 504, 429):
            # transient server error or rate-limited by infra
            print(f"Transient HTTP {resp.status_code}, attempt {attempt}. Backoff {backoff}s", file=sys.stderr)
            time.sleep(backoff)
            backoff *= 2
            continue
        else:
            # unexpected permanent error
            print(f"HTTP {resp.status_code} - {resp.text}", file=sys.stderr)
            resp.raise_for_status()
    raise RuntimeError("Max retries exceeded for GraphQL request")

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS repos (
            github_id TEXT PRIMARY KEY,
            name_with_owner TEXT NOT NULL,
            url TEXT NOT NULL,
            stars INTEGER NOT NULL,
            last_crawled TIMESTAMP NOT NULL
        );
        """)
        conn.commit()

def upsert_rows(conn, rows):
    # rows: list of tuples (github_id, name_with_owner, url, stars, last_crawled)
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur,
                       """
                       INSERT INTO repos (github_id, name_with_owner, url, stars, last_crawled)
                       VALUES %s
                       ON CONFLICT (github_id) DO UPDATE
                         SET stars = EXCLUDED.stars,
                             name_with_owner = EXCLUDED.name_with_owner,
                             url = EXCLUDED.url,
                             last_crawled = EXCLUDED.last_crawled
                       """,
                       rows)
        conn.commit()

def parse_rate_limit(j):
    rl = j.get("data", {}).get("rateLimit")
    if not rl:
        return None
    return rl

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=int, default=100000, help="Number of repos to crawl (default 100000)")
    parser.add_argument("--query", type=str, default="is:public", help="GitHub search query (default: is:public)")
    args = parser.parse_args()

    target = args.target
    q = args.query

    from urllib.parse import urlparse

    result = urlparse(DATABASE_URL)
    conn = psycopg2.connect(
        dbname=result.path[1:],       # remove leading /
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )
    ensure_table(conn)

    fetched = 0
    after = None
    now = datetime.utcnow().replace(microsecond=0).isoformat()
    # Use search query; keep it simple (public repos). You can refine to languages, etc.
    variables = {"q": q, "first": PAGE_SIZE, "after": None}

    while fetched < target:
        variables["after"] = after
        j = graphql_request(variables)
        rate = parse_rate_limit(j)
        if rate:
            remaining = rate.get("remaining")
            reset_at = rate.get("resetAt")
            cost = rate.get("cost")
            # If remaining is dangerously low, wait until reset
            if remaining is not None and remaining < 10:
                # compute sleep until resetAt (ISO8601)
                if reset_at:
                    reset_ts = datetime.fromisoformat(reset_at.replace("Z", "+00:00")).timestamp()
                    sleep_for = max(0, reset_ts - time.time()) + 5.0
                    print(f"Low remaining rate ({remaining}). Sleeping until reset in {ceil(sleep_for)}s", file=sys.stderr)
                    time.sleep(sleep_for)
                    continue

        search = j["data"]["search"]
        nodes = search["nodes"]
        page_info = search["pageInfo"]
        rows = []
        for node in nodes:
            # node can be None occasionally
            if not node:
                continue
            github_id = node["id"]
            name = node["nameWithOwner"]
            url = node["url"]
            stars = node["stargazerCount"] or 0
            rows.append((github_id, name, url, stars, datetime.utcnow()))
            fetched += 1
            if fetched >= target:
                break

        upsert_rows(conn, rows)
        print(f"Fetched total: {fetched}/{target}", file=sys.stderr)

        if fetched >= target:
            break

        if not page_info["hasNextPage"]:
            print("No more pages from search; stopping.", file=sys.stderr)
            break
        after = page_info["endCursor"]

        # Small sleep to be polite (GraphQL rateLimit already helps), can be tuned
        time.sleep(0.5)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
