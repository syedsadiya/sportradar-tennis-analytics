# 🎾 Game Analytics: Unlocking Tennis Data with SportRadar API

An enterprise-grade, end-to-end tennis intelligence and game analytics platform built with **Python 3.10+**, **SQLite** (with optional MySQL support), **SQLAlchemy**, **Sportradar Tennis v3 API**, and an interactive **Streamlit** analytical dashboard.

---

## 📑 Table of Contents
1. [Project Overview](#project-overview)
2. [Repository & Role-Based Architecture](#repository--role-based-architecture)
3. [Relational Database Schema (MySQL DDL)](#relational-database-schema-mysql-ddl)
4. [Sportradar Tennis v3 API ETL Pipelines](#sportradar-tennis-v3-api-etl-pipelines)
5. [Complete Catalog of All 20 Analytical SQL Queries](#complete-catalog-of-all-20-analytical-sql-queries)
6. [Interactive Streamlit Analytics Dashboard](#interactive-streamlit-analytics-dashboard)
7. [Installation & Quickstart Guide](#installation--quickstart-guide)
8. [Sharing Role 1 Independently](#sharing-role-1-independently)

---

## 1. Project Overview

The **Game Analytics: Unlocking Tennis Data** platform extracts, models, stores, and visualizes comprehensive professional tennis data:
- **Hierarchical Tournaments & Circuits:** Captures governing tour categories (ATP, WTA, ITF Men, ITF Women) and self-referencing tournament structures (parent Grand Slam events vs. singles/doubles/qualifier sub-events).
- **Physical Stadium & Complex Infrastructure:** Normalizes sports grounds, multi-court venues, host cities, nations, and timezones.
- **Double Competitors & Weekly Performance Rankings:** Ingests live player metrics, world rank positions, weekly movements, and points tallies.
- **Automated Master ETL Orchestration:** Features exponential backoff on HTTP 429 rate limit triggers, raw payload local disk caching to safeguard trial quotas, and idempotent database loading with `ON DUPLICATE KEY UPDATE`.
- **Live Analytical SQL Query Runner:** 20 verified SQL queries executable both in MySQL and directly from the interactive Streamlit dashboard.

---

## 2. Repository & Role-Based Architecture

The codebase is organized into **4 specialized role-based modules**, allowing team members to independently work on and share their specific role directory while maintaining seamless end-to-end integration:

```
sportradar-tennis-analytics/
├── role1_database_and_competitions/       # Role 1: Master Schema & Competitions ETL
│   ├── schema.sql                         # MySQL DDL for all 6 tables & constraints
│   ├── etl_competitions.py                # Normalized API ETL with parent-first sorting
│   └── queries_competitions.sql           # Queries 1 to 7 (Tournaments & Categories)
├── role2_complexes_and_rankings/          # Role 2: Complexes, Venues & Player Rankings
│   ├── etl_complexes.py                   # Complexes & Venues ETL with default handling
│   ├── etl_rankings.py                    # Competitors & Rankings ETL
│   └── run_all_etl.py                     # Master Orchestrator with 1.5s rate-limit pauses
├── role3_sql_and_core_app/                # Role 3: Infrastructure SQL & Streamlit App
│   ├── queries_complexes.sql              # Queries 8 to 14 (Stadiums & Complexes)
│   ├── queries_rankings.sql               # Queries 15 to 20 (Player Rankings)
│   ├── db_connection.py                   # Cached SQLAlchemy Engine & Query Runner
│   └── app.py                             # 4-Tab Interactive Streamlit Dashboard
├── role4_analytics_and_docs/              # Role 4: Analytics Engine & Consolidated SQL
│   ├── analytics_leaderboards.py          # KPI metrics, Leaderboards & Plotly generators
│   └── All_SQL_Queries.sql                # Master file combining all 20 project queries
├── data/
│   └── raw/                               # Local JSON cache preserving API quota
│       ├── competitions.json
│       ├── complexes.json
│       └── double_competitors_rankings.json
├── .env.example                           # Configuration template
├── .gitignore                             # Ignore rules (.env, caches, venvs)
├── requirements.txt                       # Project dependencies
└── README.md                              # Complete system documentation
```

---

## 3. Relational Database Schema (MySQL DDL)

The data model enforces strict 3rd Normal Form (3NF) across 6 relational tables with primary keys, foreign key constraints (with `CASCADE` and `SET NULL`), and performance indexes:

```mermaid
erDiagram
    categories ||--o{ competitions : "categorizes"
    competitions ||--o{ competitions : "parent-sub hierarchy"
    complexes ||--o{ venues : "hosts"
    competitors ||--o{ competitor_rankings : "ranked in"

    categories {
        varchar(50) category_id PK
        varchar(100) category_name
    }

    competitions {
        varchar(50) competition_id PK
        varchar(100) competition_name
        varchar(50) parent_id FK "self-referencing"
        varchar(20) type
        varchar(10) gender
        varchar(50) category_id FK
    }

    complexes {
        varchar(50) complex_id PK
        varchar(100) complex_name
    }

    venues {
        varchar(50) venue_id PK
        varchar(100) venue_name
        varchar(100) city_name
        varchar(100) country_name
        char(3) country_code
        varchar(100) timezone
        varchar(50) complex_id FK
    }

    competitors {
        varchar(50) competitor_id PK
        varchar(100) name
        varchar(100) country
        char(3) country_code
        varchar(10) abbreviation
    }

    competitor_rankings {
        int rank_id PK "AUTO_INCREMENT"
        int rank
        int movement
        int points
        int competitions_played
        varchar(50) competitor_id FK "UNIQUE"
    }
```

### Table Definitions
1. **`categories`**: Master list of tournament circuits (`category_id` PK, `category_name`).
2. **`competitions`**: Tournaments with self-referencing hierarchy (`competition_id` PK, `competition_name`, `parent_id` FK -> `competitions.competition_id`, `type`, `gender`, `category_id` FK -> `categories.category_id`).
3. **`complexes`**: Multi-court sporting complexes (`complex_id` PK, `complex_name`).
4. **`venues`**: Courts and stadiums within complexes (`venue_id` PK, `venue_name`, `city_name`, `country_name`, `country_code`, `timezone`, `complex_id` FK -> `complexes.complex_id`).
5. **`competitors`**: Professional athletes (`competitor_id` PK, `name`, `country`, `country_code`, `abbreviation`).
6. **`competitor_rankings`**: Weekly ranking snapshots (`rank_id` PK AUTO_INCREMENT, `rank`, `movement`, `points`, `competitions_played`, `competitor_id` FK UNIQUE -> `competitors.competitor_id`).

---

## 4. Sportradar Tennis v3 API ETL Pipelines

The data ingestion pipeline addresses the operational constraints of the Sportradar Tennis v3 Trial API (1 request/sec limit, strict quota boundaries):

| Pipeline Script | Target Sportradar v3 Endpoint | Normalized Destination Tables | Key Transformation Logic |
|---|---|---|---|
| `etl_competitions.py` | `.../tennis/trial/v3/en/competitions.json` | `categories`, `competitions` | Extracts categories first; topological sort ensures parent competitions (`parent_id IS NULL`) are inserted before child sub-competitions to guarantee self-referencing FK integrity. |
| `etl_complexes.py` | `.../tennis/trial/v3/en/complexes.json` | `complexes`, `venues` | Normalizes nested complexes and venue lists; cleanly populates missing optional strings (`city_name`, `country_name`, `country_code`, `timezone`) with reliable fallbacks to satisfy `NOT NULL` constraints. |
| `etl_rankings.py` | `.../tennis/trial/v3/en/double_competitors_rankings.json` | `competitors`, `competitor_rankings` | Unpacks nested rankings blocks into players and weekly ranking statistics. |
| `run_all_etl.py` | *Orchestrates all 3 endpoints* | *All 6 tables* | Executes all three ETL jobs in dependency order with a **1.5-second sleep** between requests to strictly prevent HTTP 429 rate limits. |

### API Hardening & Resilience
- **Authentication**: Transmits the API key via the official `x-api-key` HTTP header read securely from `.env`.
- **Exponential Backoff**: In the event of an HTTP 429 response, retries automatically with doubling delays (`1.5s`, `3.0s`, `6.0s`, `12.0s`).
- **Disk Caching**: Persists raw JSON payloads under `data/raw/`. If an API key is not supplied or the trial quota is exceeded, the pipeline gracefully falls back to local cache files.
- **Idempotent Upserts**: Uses MySQL `INSERT INTO ... ON DUPLICATE KEY UPDATE` to support repeat executions without duplicate key violations.

---

## 5. Complete Catalog of All 20 Analytical SQL Queries

All 20 verified SQL queries are documented below and consolidated in `role4_analytics_and_docs/All_SQL_Queries.sql`.

### Section 1: Competitions & Categories (Role 1 — `queries_competitions.sql`)
1. **Query 1: List all competitions along with their category name**
   ```sql
   SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_id, cat.category_name
   FROM competitions c
   INNER JOIN categories cat ON c.category_id = cat.category_id
   ORDER BY cat.category_name ASC, c.competition_name ASC;
   ```
2. **Query 2: Count the number of competitions in each category**
   ```sql
   SELECT cat.category_id, cat.category_name, COUNT(c.competition_id) AS total_competitions
   FROM categories cat
   LEFT JOIN competitions c ON cat.category_id = c.category_id
   GROUP BY cat.category_id, cat.category_name
   ORDER BY total_competitions DESC, cat.category_name ASC;
   ```
3. **Query 3: Find all competitions of type 'doubles'**
   ```sql
   SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name
   FROM competitions c
   INNER JOIN categories cat ON c.category_id = cat.category_id
   WHERE LOWER(c.type) = 'doubles'
   ORDER BY c.competition_name ASC;
   ```
4. **Query 4: Get competitions that belong to a specific category ('ITF Men')**
   ```sql
   SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name
   FROM competitions c
   INNER JOIN categories cat ON c.category_id = cat.category_id
   WHERE cat.category_name = 'ITF Men'
   ORDER BY c.competition_name ASC;
   ```
5. **Query 5: Identify parent competitions and their sub-competitions (self-join)**
   ```sql
   SELECT p.competition_id AS parent_competition_id, p.competition_name AS parent_competition_name,
          s.competition_id AS sub_competition_id, s.competition_name AS sub_competition_name,
          s.type AS sub_competition_type, s.gender AS sub_competition_gender
   FROM competitions s
   INNER JOIN competitions p ON s.parent_id = p.competition_id
   ORDER BY p.competition_name ASC, s.competition_name ASC;
   ```
6. **Query 6: Analyze the distribution of competition types by category**
   ```sql
   SELECT cat.category_name, c.type AS competition_type, COUNT(c.competition_id) AS competition_count
   FROM categories cat
   INNER JOIN competitions c ON cat.category_id = c.category_id
   GROUP BY cat.category_name, c.type
   ORDER BY cat.category_name ASC, competition_count DESC;
   ```
7. **Query 7: List all competitions with no parent (top-level competitions)**
   ```sql
   SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name
   FROM competitions c
   INNER JOIN categories cat ON c.category_id = cat.category_id
   WHERE c.parent_id IS NULL
   ORDER BY cat.category_name ASC, c.competition_name ASC;
   ```

### Section 2: Complexes & Venues (Role 3 — `queries_complexes.sql`)
8. **Query 8: List all venues along with their associated complex name**
   ```sql
   SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.country_code, v.timezone, c.complex_id, c.complex_name
   FROM venues v
   INNER JOIN complexes c ON v.complex_id = c.complex_id
   ORDER BY c.complex_name ASC, v.venue_name ASC;
   ```
9. **Query 9: Count the number of venues in each complex**
   ```sql
   SELECT c.complex_id, c.complex_name, COUNT(v.venue_id) AS total_venues
   FROM complexes c
   LEFT JOIN venues v ON c.complex_id = v.complex_id
   GROUP BY c.complex_id, c.complex_name
   ORDER BY total_venues DESC, c.complex_name ASC;
   ```
10. **Query 10: Get details of venues in a specific country ('Chile')**
    ```sql
    SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.country_code, v.timezone, c.complex_name
    FROM venues v
    INNER JOIN complexes c ON v.complex_id = c.complex_id
    WHERE v.country_name = 'Chile'
    ORDER BY v.city_name ASC, v.venue_name ASC;
    ```
11. **Query 11: Identify all venues and their timezones**
    ```sql
    SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.timezone
    FROM venues v
    ORDER BY v.timezone ASC, v.venue_name ASC;
    ```
12. **Query 12: Find complexes that have more than one venue (HAVING COUNT(*) > 1)**
    ```sql
    SELECT c.complex_id, c.complex_name, COUNT(v.venue_id) AS venue_count
    FROM complexes c
    INNER JOIN venues v ON c.complex_id = v.complex_id
    GROUP BY c.complex_id, c.complex_name
    HAVING COUNT(v.venue_id) > 1
    ORDER BY venue_count DESC, c.complex_name ASC;
    ```
13. **Query 13: List venues grouped by country**
    ```sql
    SELECT v.country_name, v.country_code, COUNT(v.venue_id) AS venue_count,
           GROUP_CONCAT(v.venue_name ORDER BY v.venue_name SEPARATOR ', ') AS venue_list
    FROM venues v
    GROUP BY v.country_name, v.country_code
    ORDER BY venue_count DESC, v.country_name ASC;
    ```
14. **Query 14: Find all venues for a specific complex ('Nacional')**
    ```sql
    SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.timezone, c.complex_name
    FROM venues v
    INNER JOIN complexes c ON v.complex_id = c.complex_id
    WHERE c.complex_name LIKE '%Nacional%'
    ORDER BY v.venue_name ASC;
    ```

### Section 3: Competitor Rankings (Role 3 — `queries_rankings.sql`)
15. **Query 15: Get all competitors with their rank and points**
    ```sql
    SELECT r.rank, c.competitor_id, c.name AS competitor_name, c.country, c.country_code,
           c.abbreviation, r.points, r.movement, r.competitions_played
    FROM competitor_rankings r
    INNER JOIN competitors c ON r.competitor_id = c.competitor_id
    ORDER BY r.rank ASC;
    ```
16. **Query 16: Find competitors ranked in the top 5**
    ```sql
    SELECT r.rank, c.name AS competitor_name, c.country, c.abbreviation,
           r.points, r.movement, r.competitions_played
    FROM competitor_rankings r
    INNER JOIN competitors c ON r.competitor_id = c.competitor_id
    WHERE r.rank <= 5
    ORDER BY r.rank ASC;
    ```
17. **Query 17: List competitors with no rank movement (movement = 0)**
    ```sql
    SELECT r.rank, c.name AS competitor_name, c.country, r.points, r.movement, r.competitions_played
    FROM competitor_rankings r
    INNER JOIN competitors c ON r.competitor_id = c.competitor_id
    WHERE r.movement = 0
    ORDER BY r.rank ASC;
    ```
18. **Query 18: Get the total points of competitors from a specific country ('Croatia')**
    ```sql
    SELECT c.country, c.country_code, COUNT(c.competitor_id) AS total_players,
           SUM(r.points) AS total_points, ROUND(AVG(r.points), 2) AS average_points,
           MIN(r.rank) AS best_rank
    FROM competitors c
    INNER JOIN competitor_rankings r ON c.competitor_id = r.competitor_id
    WHERE c.country = 'Croatia'
    GROUP BY c.country, c.country_code;
    ```
19. **Query 19: Count the number of competitors per country**
    ```sql
    SELECT c.country, c.country_code, COUNT(c.competitor_id) AS total_competitors,
           SUM(r.points) AS aggregate_points, ROUND(AVG(r.points), 2) AS average_points,
           MIN(r.rank) AS highest_rank
    FROM competitors c
    INNER JOIN competitor_rankings r ON c.competitor_id = r.competitor_id
    GROUP BY c.country, c.country_code
    ORDER BY total_competitors DESC, aggregate_points DESC;
    ```
20. **Query 20: Find competitors with the highest points in the current week**
    ```sql
    SELECT r.rank, c.name AS competitor_name, c.country, c.country_code,
           r.points, r.movement, r.competitions_played
    FROM competitor_rankings r
    INNER JOIN competitors c ON r.competitor_id = c.competitor_id
    WHERE r.points = (SELECT MAX(points) FROM competitor_rankings)
    ORDER BY r.rank ASC;
    ```

---

## 6. Interactive Streamlit Analytics Dashboard

The analytical web app (`role3_sql_and_core_app/app.py`) provides a responsive, dark-mode tennis analytics interface:

- **Sidebar Controls**:
  - Live MySQL database ping status (green indicator).
  - One-click **Trigger Master ETL Sync** button.
  - Multi-attribute filters: Gender, Rank Range slider (1–50), Country dropdown, Minimum Points threshold slider, Competition Format, and Tour Category.
- **Tab 1: Homepage KPI Dashboard**:
  - Executive KPI scorecards: Total Competitors, Nations Represented, World #1 Points, Competitions, Stadium Venues, Complexes.
  - Interactive Plotly charts: Points distribution histogram, category donut breakdown, and competition formats grouped bar chart.
- **Tab 2: Competitors & Details Viewer**:
  - Live search input matching player names in real time.
  - Interactive athlete grid with rank badges and movement indicators (`🔼 +2`, `🔽 -1`, `⏺ 0`).
  - **Competitor Spotlight Inspector**: Select any player to view customized `st.metric` cards showing World Rank, weekly delta, official points, and tournaments played.
- **Tab 3: Country Analytics & Dual Leaderboards**:
  - Horizontal Plotly bar chart showing nations by player count and aggregate points.
  - Country aggregation summary table.
  - Dual side-by-side leaderboards: **Top-Ranked Competitors** vs. **Highest Points Scoring Leaderboard**.
- **Tab 4: Competitions, Venues & Live SQL Runner**:
  - Tournament hierarchy explorer (Parent -> Sub-Competitions).
  - Stadiums & Venues explorer grouped by complex and host city.
  - **Live Analytical SQL Query Runner**: Dropdown menu containing all 20 required project queries. Users can inspect the SQL query, edit the query in real time, execute it against MySQL, and export results directly to CSV.

---

## 7. Installation & Quickstart Guide

### Prerequisites
- Python 3.10+
- No database installation required for the SQLite demo

### Step 1: Clone & Configure Environment
```bash
git clone <repo-url>
cd sportradar-tennis-analytics

# Copy environment template
cp .env.example .env
```

Edit `.env` to specify your database connection and Sportradar API key:
```env
SPORTRADAR_API_KEY=offline_mode
DATABASE_URL=mysql+pymysql://root:password@localhost:3306/tennis_analytics
RATE_LIMIT_SLEEP=1.5
```
*(Note: `offline_mode` loads the provided sample JSON payloads from `data/raw/`; no API key or Docker is required.)*

### Step 2: Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Initialize the SQLite Database
The master ETL pipeline creates `data/tennis.db` and all required tables automatically.

### Step 4: Run the Master ETL Pipeline
```bash
python role2_complexes_and_rankings/run_all_etl.py
```

### Step 5: Launch the Streamlit Dashboard
```bash
streamlit run role3_sql_and_core_app/app.py
```
Open your browser at `http://localhost:8501`.

---

## 8. Sharing Role 1 Independently

If you need to share **only your Role 1 folder** (`role1_database_and_competitions/`) with your classmates or evaluators, you can zip or copy just that directory:

```
role1_database_and_competitions/
├── schema.sql
├── etl_competitions.py
└── queries_competitions.sql
```

The script `etl_competitions.py` is fully self-contained:
- It can be run directly from inside `role1_database_and_competitions/` via `python etl_competitions.py`.
- It automatically resolves `.env` from either its current directory or parent directory.
- It loads raw data from `data/raw/competitions.json` or fetches live data from Sportradar with exponential backoff.
- It creates and connects to the MySQL engine independently without external role dependencies.
