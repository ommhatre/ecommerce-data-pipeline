-- ══════════════════════════════════════════════════════
-- E-Commerce Pipeline — Analytics Queries
-- Run these against the PostgreSQL warehouse to explore
-- the data produced by the PySpark pipeline
-- ══════════════════════════════════════════════════════


-- ──────────────────────────────────────────────────────
-- 1. OVERVIEW — Total pipeline metrics
-- ──────────────────────────────────────────────────────
SELECT
    COUNT(*)                                          AS total_events,
    COUNT(DISTINCT user_id)                           AS active_users,
    COUNT(*) FILTER (WHERE event_type = 'page_view')  AS total_views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS cart_additions,
    COUNT(*) FILTER (WHERE event_type = 'purchase')   AS purchases,
    ROUND(SUM(revenue)::numeric, 2)                   AS total_revenue
FROM fact_events;


-- ──────────────────────────────────────────────────────
-- 2. REVENUE BY CATEGORY
-- ──────────────────────────────────────────────────────
SELECT
    category,
    COUNT(*) FILTER (WHERE event_type = 'purchase')  AS purchases,
    ROUND(SUM(revenue)::numeric, 2)                  AS total_revenue,
    ROUND(AVG(price)::numeric, 2)                    AS avg_price
FROM fact_events
GROUP BY category
ORDER BY total_revenue DESC;


-- ──────────────────────────────────────────────────────
-- 3. TOP 10 PRODUCTS BY VIEWS
-- ──────────────────────────────────────────────────────
SELECT
    product_name,
    category,
    COUNT(*) AS total_views
FROM fact_events
WHERE event_type = 'page_view'
GROUP BY product_name, category
ORDER BY total_views DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────
-- 4. TOP 10 PRODUCTS BY REVENUE
-- ──────────────────────────────────────────────────────
SELECT
    product_name,
    category,
    COUNT(*)                        AS purchases,
    ROUND(SUM(revenue)::numeric, 2) AS total_revenue
FROM fact_events
WHERE event_type = 'purchase'
GROUP BY product_name, category
ORDER BY total_revenue DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────
-- 5. CONVERSION FUNNEL
-- Views → Cart → Purchase rates
-- ──────────────────────────────────────────────────────
SELECT
    product_name,
    COUNT(*) FILTER (WHERE event_type = 'page_view')   AS views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS cart_adds,
    COUNT(*) FILTER (WHERE event_type = 'purchase')    AS purchases,
    ROUND(
        COUNT(*) FILTER (WHERE event_type = 'purchase') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE event_type = 'page_view'), 0),
        2
    )                                                  AS conversion_rate_pct
FROM fact_events
GROUP BY product_name
ORDER BY conversion_rate_pct DESC NULLS LAST
LIMIT 10;


-- ──────────────────────────────────────────────────────
-- 6. HOURLY EVENT VOLUME
-- Useful for identifying peak traffic hours
-- ──────────────────────────────────────────────────────
SELECT
    event_hour,
    COUNT(*)                                          AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'purchase')  AS purchases,
    ROUND(SUM(revenue)::numeric, 2)                  AS revenue
FROM fact_events
GROUP BY event_hour
ORDER BY event_hour;


-- ──────────────────────────────────────────────────────
-- 7. TOP SPENDING USERS
-- ──────────────────────────────────────────────────────
SELECT
    user_id,
    COUNT(*) FILTER (WHERE event_type = 'purchase')  AS total_purchases,
    ROUND(SUM(revenue)::numeric, 2)                  AS total_spent
FROM fact_events
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────
-- 8. DAILY REVENUE TREND
-- ──────────────────────────────────────────────────────
SELECT
    event_date,
    COUNT(*) FILTER (WHERE event_type = 'purchase')  AS purchases,
    ROUND(SUM(revenue)::numeric, 2)                  AS daily_revenue,
    COUNT(DISTINCT user_id)                          AS active_users
FROM fact_events
GROUP BY event_date
ORDER BY event_date DESC;