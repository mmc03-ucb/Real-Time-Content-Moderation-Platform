-- StreamGuard's durable state. Loaded automatically the first time MySQL starts.

CREATE TABLE IF NOT EXISTS rules (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  rule_type  VARCHAR(32)  NOT NULL,  -- keyword, regex, link, new_account, frequency
  pattern    VARCHAR(512) NULL,
  threshold  DOUBLE       NULL,
  action     VARCHAR(16)  NOT NULL,  -- allow, delete, shadow, escalate
  priority   INT          NOT NULL DEFAULT 100,  -- lower runs first
  enabled    TINYINT(1)   NOT NULL DEFAULT 1,
  stream_id  VARCHAR(64)  NULL,      -- NULL means every stream
  version    INT          NOT NULL DEFAULT 1,
  updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- One row per message we made a call on. msg_id is unique so a message that
-- gets delivered twice cannot be recorded twice.
CREATE TABLE IF NOT EXISTS decisions (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  msg_id      CHAR(36)     NOT NULL UNIQUE,
  stream_id   VARCHAR(64)  NOT NULL,
  user_id     VARCHAR(64)  NOT NULL,
  action      VARCHAR(16)  NOT NULL,
  reason_code VARCHAR(32)  NOT NULL,
  rule_id     INT          NULL,
  ml_score    DOUBLE       NULL,
  strategy    VARCHAR(16)  NOT NULL DEFAULT 'A',
  latency_ms  DOUBLE       NOT NULL DEFAULT 0,
  created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_decisions_created (created_at),
  INDEX idx_decisions_strategy (strategy, action)
);

-- The human review queue.
CREATE TABLE IF NOT EXISTS review_items (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  msg_id         CHAR(36)    NOT NULL UNIQUE,
  stream_id      VARCHAR(64) NOT NULL,
  user_id        VARCHAR(64) NOT NULL,
  text           TEXT        NOT NULL,
  ml_score       DOUBLE      NULL,
  rule_hits_json JSON        NULL,
  strategy       VARCHAR(16) NOT NULL DEFAULT 'A',
  status         VARCHAR(16) NOT NULL DEFAULT 'pending',  -- pending, claimed, done
  reviewer       VARCHAR(64) NULL,
  decision       VARCHAR(16) NULL,  -- what the human chose: allow or delete
  created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  claimed_at     TIMESTAMP   NULL,
  decided_at     TIMESTAMP   NULL,
  INDEX idx_review_status (status, created_at)
);

-- Durable copy of user reputation. The live counter lives in Redis.
CREATE TABLE IF NOT EXISTS users_risk (
  user_id           VARCHAR(64) PRIMARY KEY,
  risk_score        DOUBLE      NOT NULL DEFAULT 0,
  violations        INT         NOT NULL DEFAULT 0,
  last_violation_at TIMESTAMP   NULL
);

-- The A/B buckets. Each one carries its own thresholds.
CREATE TABLE IF NOT EXISTS strategies (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(16) NOT NULL UNIQUE,
  config_json JSON        NOT NULL,
  active      TINYINT(1)  NOT NULL DEFAULT 1
);
