-- ══════════════════════════════════════════════════════════════
-- E-COMMERCE PIPELINE — PostgreSQL Warehouse Schema
-- ══════════════════════════════════════════════════════════════

-- Drop tables if they exist so we can recreate cleanly
-- CASCADE drops any dependent objects too
DROP TABLE IF EXISTS user_metrics     CASCADE;
DROP TABLE IF EXISTS category_metrics CASCADE;
DROP TABLE IF EXISTS product_metrics  CASCADE;
DROP TABLE IF EXISTS fact_events      CASCADE;


-- ──────────────────────────────────────────────────────────────
-- TABLE 1: fact_events
-- Every single user action — the source of truth
-- One row = one event
-- ──────────────────────────────────────────────────────────────
CREATE TABLE fact_events (
    event_id        VARCHAR(100)   PRIMARY KEY,
    user_id         INTEGER        NOT NULL,
    event_type      VARCHAR(50)    NOT NULL
                        CHECK (event_type IN ('page_view','add_to_cart','purchase')),
    product_id      INTEGER        NOT NULL,
    product_name    VARCHAR(200)   NOT NULL,
    category        VARCHAR(100)   NOT NULL,
    price           NUMERIC(10,2)  NOT NULL CHECK (price > 0),
    quantity        INTEGER        NOT NULL DEFAULT 1,
    revenue         NUMERIC(10,2)  NOT NULL DEFAULT 0,
    event_date      DATE           NOT NULL,
    event_hour      INTEGER        NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    timestamp       TIMESTAMPTZ    NOT NULL,
    processed_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Index on common query patterns
-- An index is like a book's index — it lets PostgreSQL
-- find rows instantly without scanning the whole table
CREATE INDEX idx_fact_events_date      ON fact_events (event_date);
CREATE INDEX idx_fact_events_user      ON fact_events (user_id);
CREATE INDEX idx_fact_events_product   ON fact_events (product_id);
CREATE INDEX idx_fact_events_category  ON fact_events (category);
CREATE INDEX idx_fact_events_type      ON fact_events (event_type);


-- ──────────────────────────────────────────────────────────────
-- TABLE 2: product_metrics
-- Aggregated stats per product per hour
-- One row = one product's stats for one hour window
-- ──────────────────────────────────────────────────────────────
CREATE TABLE product_metrics (
    id                    SERIAL PRIMARY KEY,
    product_id            INTEGER        NOT NULL,
    product_name          VARCHAR(200)   NOT NULL,
    category              VARCHAR(100)   NOT NULL,
    total_views           INTEGER        NOT NULL DEFAULT 0,
    total_cart_additions  INTEGER        NOT NULL DEFAULT 0,
    total_purchases       INTEGER        NOT NULL DEFAULT 0,
    total_revenue         NUMERIC(12,2)  NOT NULL DEFAULT 0,
    window_start          TIMESTAMPTZ    NOT NULL,
    loaded_at             TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_product_metrics_product ON product_metrics (product_id);
CREATE INDEX idx_product_metrics_window  ON product_metrics (window_start);
CREATE INDEX idx_product_metrics_cat     ON product_metrics (category);


-- ──────────────────────────────────────────────────────────────
-- TABLE 3: category_metrics
-- Revenue and activity rolled up by category per hour
-- One row = one category's stats for one hour window
-- ──────────────────────────────────────────────────────────────
CREATE TABLE category_metrics (
    id                    SERIAL PRIMARY KEY,
    category              VARCHAR(100)   NOT NULL,
    total_views           INTEGER        NOT NULL DEFAULT 0,
    total_cart_additions  INTEGER        NOT NULL DEFAULT 0,
    total_purchases       INTEGER        NOT NULL DEFAULT 0,
    total_revenue         NUMERIC(12,2)  NOT NULL DEFAULT 0,
    window_start          TIMESTAMPTZ    NOT NULL,
    loaded_at             TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_category_metrics_cat    ON category_metrics (category);
CREATE INDEX idx_category_metrics_window ON category_metrics (window_start);


-- ──────────────────────────────────────────────────────────────
-- TABLE 4: user_metrics
-- Per-user activity summary
-- One row = one user's stats from one processed batch
-- ──────────────────────────────────────────────────────────────
CREATE TABLE user_metrics (
    id                SERIAL PRIMARY KEY,
    user_id           INTEGER        NOT NULL,
    total_events      INTEGER        NOT NULL DEFAULT 0,
    total_purchases   INTEGER        NOT NULL DEFAULT 0,
    total_spent       NUMERIC(12,2)  NOT NULL DEFAULT 0,
    first_seen        TIMESTAMPTZ,
    last_seen         TIMESTAMPTZ,
    loaded_at         TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_user_metrics_user   ON user_metrics (user_id);
CREATE INDEX idx_user_metrics_loaded ON user_metrics (loaded_at);


-- ──────────────────────────────────────────────────────────────
-- VERIFY: show all created tables
-- ──────────────────────────────────────────────────────────────
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;