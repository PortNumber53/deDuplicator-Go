ALTER TABLE files ADD COLUMN root_folder TEXT;
-- You may want to backfill this with an UPDATE if you have a default or can infer it for existing rows.
