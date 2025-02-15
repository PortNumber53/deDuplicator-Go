CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    path TEXT NOT NULL,
    host TEXT NOT NULL,
    hash TEXT,
    size BIGINT,
    last_hashed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(path, host),
    FOREIGN KEY (host) REFERENCES hosts(name)
); 