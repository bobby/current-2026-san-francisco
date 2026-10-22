-- The tickets mock (docs/design.md §9.2): ServiceNow's incident table, with
-- its journal (work notes and comments) in a table of its own.

CREATE TABLE incident (
    sys_id text PRIMARY KEY,
    -- INC0010001, INC0010002, … from a sequence the table owns, so
    -- `TRUNCATE … RESTART IDENTITY` (the director's reset) restarts it.
    number text NOT NULL UNIQUE,
    short_description text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    state smallint NOT NULL DEFAULT 1,
    priority smallint NOT NULL DEFAULT 4,
    impact smallint NOT NULL DEFAULT 3,
    urgency smallint NOT NULL DEFAULT 3,
    category text NOT NULL DEFAULT '',
    assignment_group text NOT NULL DEFAULT '',
    assigned_to text NOT NULL DEFAULT '',
    caller_id text NOT NULL DEFAULT '',
    cmdb_ci text NOT NULL DEFAULT '',
    correlation_id text NOT NULL DEFAULT '',
    close_code text NOT NULL DEFAULT '',
    close_notes text NOT NULL DEFAULT '',
    opened_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    closed_at timestamptz,
    sys_created_on timestamptz NOT NULL DEFAULT now(),
    sys_created_by text NOT NULL,
    sys_updated_on timestamptz NOT NULL DEFAULT now(),
    sys_updated_by text NOT NULL,
    sys_mod_count integer NOT NULL DEFAULT 0
);
CREATE SEQUENCE incident_number START 10001 OWNED BY incident.number;
ALTER TABLE incident ALTER COLUMN number SET DEFAULT 'INC' || lpad(nextval('incident_number')::text, 7, '0');
CREATE INDEX incident_cmdb_ci ON incident (cmdb_ci, sys_created_on);
CREATE INDEX incident_correlation_id ON incident (correlation_id);

CREATE TABLE journal (
    id bigserial PRIMARY KEY,
    sys_id text NOT NULL REFERENCES incident ON DELETE CASCADE,
    element text NOT NULL CHECK (element IN ('work_notes', 'comments')),
    value text NOT NULL,
    created_by text NOT NULL,
    created_on timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX journal_incident ON journal (sys_id, created_on);
