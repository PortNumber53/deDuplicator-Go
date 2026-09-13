-- Tracks the last time a group operation traversed a file. NULL means that
-- the file has not been visited yet, allowing group work to prioritize it.
ALTER TABLE files ADD COLUMN updated_at TIMESTAMP;

CREATE INDEX idx_files_updated_at ON files(updated_at);
