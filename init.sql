-- ============================================================
-- CryptoPulse - Auto Schema Init
-- Chạy tự động khi MySQL container khởi động lần đầu
-- ============================================================

CREATE DATABASE IF NOT EXISTS cryptopulse
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE cryptopulse;

-- ─────────────────────────────────────────
-- Tạo bảng macro cho từng timeframe
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kline_1m (
    symbol       VARCHAR(20)  NOT NULL,
    open_time    BIGINT       NOT NULL,
    open_price   DOUBLE       NOT NULL,
    high_price   DOUBLE       NOT NULL,
    low_price    DOUBLE       NOT NULL,
    close_price  DOUBLE       NOT NULL,
    volume       DOUBLE       NOT NULL,
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, open_time),
    INDEX idx_symbol (symbol),
    INDEX idx_time   (open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS kline_5m LIKE kline_1m;
CREATE TABLE IF NOT EXISTS kline_15m LIKE kline_1m;
CREATE TABLE IF NOT EXISTS kline_1h LIKE kline_1m;
CREATE TABLE IF NOT EXISTS kline_4h LIKE kline_1m;
CREATE TABLE IF NOT EXISTS kline_1d LIKE kline_1m;
CREATE TABLE IF NOT EXISTS kline_1w LIKE kline_1m;

-- ─────────────────────────────────────────
-- Bảng ticker realtime (Redis fallback)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ticker_latest (
    symbol       VARCHAR(20)  NOT NULL PRIMARY KEY,
    price        DOUBLE       NOT NULL,
    change_pct   DOUBLE       NOT NULL DEFAULT 0,
    volume_24h   DOUBLE       NOT NULL DEFAULT 0,
    high_24h     DOUBLE       NOT NULL DEFAULT 0,
    low_24h      DOUBLE       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
