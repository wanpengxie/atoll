# lightcone 重构：数据库表结构

> 所属设计文档：[lightcone-redesign.md](./lightcone-redesign.md)
> 最后更新：2026-04-19

---

## 新增表

```sql
-- Goal State（每个 team 可有多个版本，仅一个 active）
-- 只保存活跃目标快照；decisions / corrections / blockers 走 append-only 表。
CREATE TABLE team_goal_state (
  id             VARCHAR(36)  PRIMARY KEY,
  team_id        VARCHAR(36)  NOT NULL,
  version        INT          NOT NULL DEFAULT 1,
  status         ENUM('active','archived') DEFAULT 'active',
  goal           TEXT         NOT NULL,
  constraints    JSON         NOT NULL DEFAULT '[]',
  current_phase  VARCHAR(255),
  created_at     DATETIME     DEFAULT NOW(),
  updated_at     DATETIME     DEFAULT NOW(),
  UNIQUE KEY (team_id, version)
);

-- Goal State 决策日志（Orchestrator append-only）
CREATE TABLE goal_decisions (
  id              VARCHAR(36)  PRIMARY KEY,
  goal_state_id   VARCHAR(36)  NOT NULL,
  team_id         VARCHAR(36)  NOT NULL,
  actor_agent_id  VARCHAR(36),
  content         TEXT         NOT NULL,
  rationale       TEXT,
  status          ENUM('active','superseded','archived') DEFAULT 'active',
  superseded_by   VARCHAR(36),
  created_at      DATETIME     DEFAULT NOW()
);

-- Goal State 纠偏日志（Orchestrator append-only）
CREATE TABLE goal_corrections (
  id              VARCHAR(36)  PRIMARY KEY,
  goal_state_id   VARCHAR(36)  NOT NULL,
  team_id         VARCHAR(36)  NOT NULL,
  actor_agent_id  VARCHAR(36),
  deviation       TEXT         NOT NULL,
  correction      TEXT         NOT NULL,
  severity        ENUM('minor','major') DEFAULT 'minor',
  status          ENUM('active','resolved','superseded','archived') DEFAULT 'active',
  superseded_by   VARCHAR(36),
  created_at      DATETIME     DEFAULT NOW(),
  resolved_at     DATETIME
);

-- Goal State 阻塞项（Worker 可创建，Orchestrator 关闭）
CREATE TABLE goal_blockers (
  id              VARCHAR(36)  PRIMARY KEY,
  goal_state_id   VARCHAR(36)  NOT NULL,
  team_id         VARCHAR(36)  NOT NULL,
  source_agent_id VARCHAR(36),
  task_id         VARCHAR(36),
  content         TEXT         NOT NULL,
  severity        ENUM('info','warn','blocker') DEFAULT 'warn',
  status          ENUM('open','closed','archived') DEFAULT 'open',
  resolution      TEXT,
  created_at      DATETIME     DEFAULT NOW(),
  closed_at       DATETIME
);

-- Orchestrator 绑定（每个 team 一个 Orchestrator）
CREATE TABLE team_orchestrator (
  id          VARCHAR(36)  PRIMARY KEY,
  team_id     VARCHAR(36)  NOT NULL UNIQUE,
  agent_id    VARCHAR(36)  NOT NULL,
  created_at  DATETIME     DEFAULT NOW()
);

-- Task Dispatch（Orchestrator → Worker 任务记录）
CREATE TABLE task_dispatch (
  id                VARCHAR(36)   PRIMARY KEY,
  team_id           VARCHAR(36)   NOT NULL,
  orchestrator_id   VARCHAR(36)   NOT NULL,
  worker_agent_id   VARCHAR(36)   NOT NULL,
  task_prompt       TEXT          NOT NULL,
  status            ENUM('created','assigned','in_progress','in_review','done','blocked')
                    DEFAULT 'created',
  result            MEDIUMTEXT,
  notification_xml  TEXT,
  context_bundle_id VARCHAR(36),
  created_at        DATETIME      DEFAULT NOW(),
  completed_at      DATETIME
);

-- Context Source（所有长期 context 的来源索引）
CREATE TABLE context_sources (
  id           VARCHAR(36)  PRIMARY KEY,
  team_id      VARCHAR(36)  NOT NULL,
  source_type  ENUM('human','orchestrator','worker','tool','system','document') NOT NULL,
  source_id    VARCHAR(255),
  title        VARCHAR(255),
  uri          TEXT,
  created_at   DATETIME     DEFAULT NOW()
);

-- 统一 Context Item（长期治理对象）
CREATE TABLE context_items (
  id             VARCHAR(36)  PRIMARY KEY,
  item_version   INT          NOT NULL DEFAULT 1,
  content_hash   VARCHAR(128),
  team_id         VARCHAR(36)  NOT NULL,
  task_id         VARCHAR(36),
  scope_type      ENUM('organization','user','team','task') NOT NULL DEFAULT 'team',
  scope_id        VARCHAR(36)  NOT NULL,
  type            ENUM('goal','constraint','decision','correction','knowledge','skill','memory','working','preference','norm','blocker')
                  NOT NULL,
  layer           ENUM('frozen','stable','evolving','ephemeral') NOT NULL,
  section         ENUM('goal','state','skill','knowledge','memory','working') NOT NULL,
  title           VARCHAR(255),
  content         MEDIUMTEXT   NOT NULL,
  summary         TEXT,
  rendered_text   MEDIUMTEXT,
  language        VARCHAR(32),
  source_type     ENUM('human','orchestrator','worker','tool','system','document') NOT NULL,
  source_id       VARCHAR(36),
  source_version  VARCHAR(64),
  source_uri      TEXT,
  source_risk     ENUM('trusted','normal','untrusted','hostile') DEFAULT 'normal',
  projection_mode ENUM('source_of_truth','projection') DEFAULT 'projection',
  authority       ENUM('human','system','orchestrator','agent','tool') NOT NULL,
  confidence      DECIMAL(4,3),
  status          ENUM('candidate','active','superseded','archived','rejected','expired') DEFAULT 'candidate',
  visibility      ENUM('private','team','organization','system') DEFAULT 'team',
  tags            JSON         NOT NULL DEFAULT '[]',
  keywords        JSON         NOT NULL DEFAULT '[]',
  embedding_ref   VARCHAR(255),
  priority        INT          NOT NULL DEFAULT 0,
  expires_at      DATETIME,
  superseded_by   VARCHAR(36),
  promoted_at     DATETIME,
  promoted_by     VARCHAR(36),
  archived_at     DATETIME,
  rejected_at     DATETIME,
  usage_count     INT          NOT NULL DEFAULT 0,
  last_injected_at DATETIME,
  last_used_at    DATETIME,
  last_eval_status VARCHAR(64),
  created_at      DATETIME     DEFAULT NOW(),
  updated_at      DATETIME     DEFAULT NOW()
);

-- 每次 agent 调用实际注入的 Context Bundle
CREATE TABLE context_bundles (
  id             VARCHAR(36)  PRIMARY KEY,
  team_id         VARCHAR(36)  NOT NULL,
  agent_id        VARCHAR(36)  NOT NULL,
  agent_role      ENUM('orchestrator','worker') DEFAULT 'worker',
  task_id         VARCHAR(36),
  agent_run_id    VARCHAR(36),
  goal_state_id   VARCHAR(36),
  task_desc       TEXT,
  model           VARCHAR(255),
  retrieval_profile VARCHAR(64),
  latency_profile VARCHAR(64),
  assembly_policy_version VARCHAR(64),
  token_budget    INT,
  rendered_prompt MEDIUMTEXT,
  redaction_status ENUM('none','partial','full') DEFAULT 'none',
  prompt_snapshot_visibility ENUM('private','team','admin','system') DEFAULT 'system',
  retention_until DATETIME,
  created_at      DATETIME     DEFAULT NOW()
);

-- Bundle 内的 item 明细，用于审计和回放
CREATE TABLE context_bundle_items (
  id              VARCHAR(36)  PRIMARY KEY,
  bundle_id       VARCHAR(36)  NOT NULL,
  context_item_id VARCHAR(36),
  item_version    INT,
  content_hash    VARCHAR(128),
  section         ENUM('goal','state','skill','knowledge','memory','working') NOT NULL,
  selection_status ENUM('included','skipped_timeout','skipped_conflict','skipped_low_relevance','skipped_policy','skipped_risk')
                  DEFAULT 'included',
  rank_order      INT          NOT NULL,
  token_count     INT,
  inclusion_reason TEXT,
  skip_reason     TEXT,
  conflicted_with VARCHAR(36),
  conflict_winner VARCHAR(36),
  rendered_text_snapshot MEDIUMTEXT,
  created_at      DATETIME     DEFAULT NOW()
);

-- Context 使用反馈，用于 eval 和检索优化
CREATE TABLE context_usage_events (
  id              VARCHAR(36)  PRIMARY KEY,
  bundle_id       VARCHAR(36)  NOT NULL,
  context_item_id VARCHAR(36),
  event_type      ENUM('injected','referenced','ignored','conflicted','caused_failure','missing_critical','contamination_detected','prompt_injection_detected','risk_skipped')
                  NOT NULL,
  details         TEXT,
  created_at      DATETIME     DEFAULT NOW()
);

-- Context Eval 结果（以 Context Bundle 为基本样本）
CREATE TABLE context_eval_results (
  id             VARCHAR(36)  PRIMARY KEY,
  bundle_id      VARCHAR(36)  NOT NULL,
  context_item_id VARCHAR(36),
  category       ENUM('retrieval_quality','assembly_quality','context_usage','failure_attribution','compression_quality','lifecycle_quality','human_trust_control')
                 NOT NULL,
  metric_name    VARCHAR(128) NOT NULL,
  signal_source  ENUM('system','agent','orchestrator','human','offline_replay') NOT NULL,
  severity       ENUM('info','warn','critical') DEFAULT 'info',
  result         JSON         NOT NULL DEFAULT '{}',
  recommended_action ENUM('none','adjust_retrieval','adjust_priority','review_lifecycle','adjust_compression','adjust_risk','adjust_assembly_policy')
                 DEFAULT 'none',
  action_status  ENUM('pending','applied','rejected','ignored') DEFAULT 'pending',
  created_at     DATETIME     DEFAULT NOW(),
  reviewed_at    DATETIME,
  reviewed_by    VARCHAR(36)
);

-- Knowledge Base
CREATE TABLE team_knowledge (
  id         VARCHAR(36)   PRIMARY KEY,
  team_id    VARCHAR(36)   NOT NULL,
  content    MEDIUMTEXT    NOT NULL,
  source_id  VARCHAR(36),
  authority  ENUM('human','orchestrator','agent','tool') DEFAULT 'orchestrator',
  status     ENUM('candidate','active','superseded','archived','rejected') DEFAULT 'candidate',
  tags       JSON          NOT NULL DEFAULT '[]',
  embedding  JSON,
  source     VARCHAR(255),
  expires_at DATETIME,
  created_at DATETIME      DEFAULT NOW()
);

-- Session Summaries（压缩历史）
-- 摘要必须保留覆盖范围和结构化字段，避免把假设固化为事实。
CREATE TABLE session_summaries (
  agent_id      VARCHAR(36)  NOT NULL,
  team_id       VARCHAR(36)  NOT NULL,
  summary       MEDIUMTEXT   NOT NULL,
  confirmed_facts JSON        NOT NULL DEFAULT '[]',
  user_decisions  JSON        NOT NULL DEFAULT '[]',
  constraints_mentions JSON    NOT NULL DEFAULT '[]',
  assumptions     JSON        NOT NULL DEFAULT '[]',
  open_questions  JSON        NOT NULL DEFAULT '[]',
  open_conflicts  JSON         NOT NULL DEFAULT '[]',
  rejected_options JSON       NOT NULL DEFAULT '[]',
  risks           JSON        NOT NULL DEFAULT '[]',
  next_actions    JSON        NOT NULL DEFAULT '[]',
  compression_warnings JSON   NOT NULL DEFAULT '[]',
  covers_until  BIGINT,
  source_range_start BIGINT,
  source_range_end   BIGINT,
  source_message_ids JSON      NOT NULL DEFAULT '[]',
  source_bundle_ids  JSON      NOT NULL DEFAULT '[]',
  review_status ENUM('candidate','reviewed','rejected','promoted') DEFAULT 'candidate',
  reviewed_by   VARCHAR(36),
  reviewed_at   DATETIME,
  created_at    DATETIME     DEFAULT NOW(),
  PRIMARY KEY (agent_id, team_id)
);
```

---

## 现有表变更

```sql
-- agents 表新增字段
ALTER TABLE agents ADD COLUMN role        ENUM('orchestrator','worker') DEFAULT 'worker';
ALTER TABLE agents ADD COLUMN spawn_mode  ENUM('fixed','dynamic') DEFAULT 'fixed';
ALTER TABLE agents ADD COLUMN template_id VARCHAR(36);

-- teams 表新增字段
ALTER TABLE teams ADD COLUMN interaction_mode ENUM('orchestrator','transparent','hybrid') DEFAULT 'orchestrator';

-- agent_memory 表新增字段（向量检索）
ALTER TABLE agent_memory ADD COLUMN embedding JSON;

-- skills 表新增字段（向量检索）
ALTER TABLE skills ADD COLUMN embedding JSON;
```
