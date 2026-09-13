DROP INDEX IF EXISTS idx_files_updated_at;

ALTER TABLE files DROP COLUMN IF EXISTS updated_at;
