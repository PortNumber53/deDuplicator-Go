ALTER TABLE hosts ADD COLUMN IF NOT EXISTS settings JSONB DEFAULT '{}'::jsonb;
