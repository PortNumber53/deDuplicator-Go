-- Path groups table to associate paths across hosts
CREATE TABLE path_groups (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    min_copies INT DEFAULT 2,
    max_copies INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Junction table linking host paths to groups
CREATE TABLE path_group_members (
    id SERIAL PRIMARY KEY,
    group_id INT NOT NULL REFERENCES path_groups(id) ON DELETE CASCADE,
    host_name TEXT NOT NULL REFERENCES hosts(name) ON DELETE CASCADE,
    friendly_path TEXT NOT NULL,
    priority INT DEFAULT 100,
    UNIQUE(group_id, host_name, friendly_path),
    UNIQUE(host_name, friendly_path)
);

CREATE INDEX idx_path_group_members_group ON path_group_members(group_id);
CREATE INDEX idx_path_group_members_host ON path_group_members(host_name, friendly_path);
