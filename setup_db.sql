-- setup_db.sql
CREATE TABLE IF NOT EXISTS repos (
  github_id TEXT PRIMARY KEY,
  name_with_owner TEXT NOT NULL,
  url TEXT NOT NULL,
  stars INTEGER NOT NULL,
  last_crawled TIMESTAMP NOT NULL
);

-- small index to support queries by stars if needed
CREATE INDEX IF NOT EXISTS idx_repos_stars ON repos (stars DESC);
