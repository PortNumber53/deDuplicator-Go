-- Run during a maintenance window: these transactional builds block writes.
-- Supports duplicate-size aggregation for the case-insensitive host filter.
CREATE INDEX idx_files_hostname_size ON files (LOWER(hostname), size)
    WHERE size IS NOT NULL;

-- Supports the default unhashed-file scan and its id bookmark.
CREATE INDEX idx_files_unhashed_hostname_id ON files (LOWER(hostname), id)
    WHERE hash IS NULL;
