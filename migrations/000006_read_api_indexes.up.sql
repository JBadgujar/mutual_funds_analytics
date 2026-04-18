CREATE INDEX IF NOT EXISTS idx_funds_active_name_prefix
    ON funds (active, name text_pattern_ops);

CREATE INDEX IF NOT EXISTS idx_funds_active_category_name_prefix
    ON funds (active, category, name text_pattern_ops);
