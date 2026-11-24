CREATE TABLE IF NOT EXISTS categories (
    category_id TEXT PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS competitions (
    competition_id TEXT PRIMARY KEY,
    competition_name TEXT NOT NULL,
    parent_id TEXT,
    type TEXT NOT NULL,
    gender TEXT NOT NULL,
    category_id TEXT NOT NULL,
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    FOREIGN KEY (parent_id) REFERENCES competitions(competition_id)
);

CREATE TABLE IF NOT EXISTS complexes (
    complex_id TEXT PRIMARY KEY,
    complex_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS venues (
    venue_id TEXT PRIMARY KEY,
    venue_name TEXT NOT NULL,
    city_name TEXT NOT NULL,
    country_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    timezone TEXT NOT NULL,
    complex_id TEXT NOT NULL,
    FOREIGN KEY (complex_id) REFERENCES complexes(complex_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS competitors (
    competitor_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    country_code TEXT NOT NULL,
    abbreviation TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS competitor_rankings (
    rank_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rank INTEGER NOT NULL,
    movement INTEGER NOT NULL,
    points INTEGER NOT NULL,
    competitions_played INTEGER NOT NULL,
    competitor_id TEXT NOT NULL UNIQUE,
    FOREIGN KEY (competitor_id) REFERENCES competitors(competitor_id) ON DELETE CASCADE
);
