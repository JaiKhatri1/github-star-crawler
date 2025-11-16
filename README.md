# GitHub Star Crawler

A minimal Python crawler that fetches public GitHub repositories and their star counts using GitHub's GraphQL API.  
The data is stored in **PostgreSQL**, with support for daily updates and exporting results to CSV for analysis.

---

## Features

- Fetch repositories (via GraphQL) and their star counts
- Handles **rate limits** and transient errors with exponential backoff
- Stores data in PostgreSQL with **upsert** (updates star counts automatically)
- Export repository data to **CSV** for reporting or analysis
- Easy to scale and extend for additional metadata like issues, PRs, comments, and reviews

---

## Requirements

- Python 3.13+
- PostgreSQL 15+
- Packages: `requests`, `psycopg2-binary`, `python-dotenv`, `pandas`

Install dependencies:

```bash
pip install -r requirements.txt
