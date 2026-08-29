-- Starting ruleset and A/B buckets, so the platform does something useful
-- the moment it boots.

INSERT INTO rules (name, rule_type, pattern, threshold, action, priority, stream_id) VALUES
  ('slur list',            'keyword', 'idiot,moron,loser,worthless,trash', NULL, 'delete',   10,  NULL),
  ('scam phrases',         'regex',   '(free skins|make \\$[0-9]+|dm me for)', NULL, 'delete', 20, NULL),
  ('new accounts, no links','new_account', NULL, 7,    'shadow',   30,  NULL),
  ('link flood',           'link',    NULL,     2,     'escalate', 40,  NULL),
  ('per user speed limit', 'frequency', NULL,   10,    'shadow',   50,  NULL),
  ('no links in stream_0', 'link',    NULL,     0,     'shadow',   35,  'stream_0');

-- Strategy A is today's production settings. Strategy B is the challenger:
-- it trusts the model more, which should delete more and escalate less.
INSERT INTO strategies (name, config_json, active) VALUES
  ('A', '{"delete_threshold": 0.9, "escalate_threshold": 0.6, "risk_bonus": 0.1}', 1),
  ('B', '{"delete_threshold": 0.8, "escalate_threshold": 0.5, "risk_bonus": 0.15}', 1);
