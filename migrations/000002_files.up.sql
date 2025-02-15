CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    path TEXT NOT NULL,
    hostname TEXT NOT NULL,
    hash TEXT,
    size BIGINT,
    last_hashed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(path, hostname),
    FOREIGN KEY (hostname) REFERENCES hosts(hostname)
); 