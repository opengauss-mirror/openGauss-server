/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * schema.sql
 *    Schema for DB4AI.Snapshot functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/dbmind/db4ai/snapshots/schema.sql
 *
 * -------------------------------------------------------------------------
 */

GRANT USAGE ON SCHEMA db4ai TO PUBLIC;

--CREATE SEQUENCE db4ai.snapshot_sequence; -- openGauss BUG: cannot create sequences in initdb
--GRANT USAGE ON SEQUENCE db4ai.snapshot_sequence TO db4ai;

CREATE TYPE db4ai.snapshot_name AS ("schema" NAME, "name" NAME); -- openGauss BUG: array type not created during initdb

CREATE TABLE IF NOT EXISTS db4ai.snapshot
(
    id BIGINT UNIQUE,                           -- snapshot id (surrogate key)
    parent_id BIGINT,                           -- parent snapshot id (references snapshot.id)
    matrix_id BIGINT,                           -- matrix id from CSS snapshots, else NULL
                                                -- (references snapshot.id)
    root_id BIGINT,                             -- id of the initial snapshot, constructed via
                                                -- db4ai.create_snapshot() from operational data
                                                -- (references snapshot.id)
    schema NAME NOT NULL,                       -- schema where the snapshot view is exported
    name NAME NOT NULL,                         -- name of the snapshot, including version postfix
    owner NAME NOT NULL,                        -- name of the user who created this snapshot
    commands TEXT[] NOT NULL,                   -- complete list of SQL statements documenting how
                                                -- to generate this snapshot from its ancestor
    comment TEXT,                               -- description of the snapshot
    published BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE, iff the snapshot is currently published
    archived BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE, iff the snapshot is currently archived
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,-- timestamp of snapshot creation date
    row_count BIGINT NOT NULL,                  -- number of rows in this snapshot
    PRIMARY KEY (schema, name)
) /* DISTRIBUTE BY REPLICATION */;
COMMENT ON TABLE db4ai.snapshot IS                'system catalog of meta-data on DB4AI snapshots';

COMMENT ON COLUMN db4ai.snapshot.id IS            'snapshot id (surrogate key)';
COMMENT ON COLUMN db4ai.snapshot.parent_id IS     'parent snapshot id (references snapshot.id)';
COMMENT ON COLUMN db4ai.snapshot.matrix_id IS    E'matrix id from CSS snapshots, else NULL\n'
                                                  '(references snapshot.id)';
COMMENT ON COLUMN db4ai.snapshot.root_id IS      E'id of the initial snapshot, constructed via\n'
                                                  'db4ai.create_snapshot() from operational data\n'
                                                  '(references snapshot.id)';
COMMENT ON COLUMN db4ai.snapshot.schema IS        'schema where the snapshot view is exported';
COMMENT ON COLUMN db4ai.snapshot.name IS          'name of the snapshot, including version postfix';
COMMENT ON COLUMN db4ai.snapshot.owner IS         'name of the user who created this snapshot';
COMMENT ON COLUMN db4ai.snapshot.commands IS     E'complete list of SQL statements documenting how\n'
                                                  'to generate this snapshot from its ancestor';
COMMENT ON COLUMN db4ai.snapshot.comment IS       'description of the snapshot';
COMMENT ON COLUMN db4ai.snapshot.published IS     'TRUE, iff the snapshot is currently published';
COMMENT ON COLUMN db4ai.snapshot.archived IS      'TRUE, iff the snapshot is currently archived';
COMMENT ON COLUMN db4ai.snapshot.created IS       'timestamp of snapshot creation date';

-- public read-only access to snapshot catalog
REVOKE ALL PRIVILEGES ON db4ai.snapshot FROM PUBLIC;
GRANT SELECT ON db4ai.snapshot TO PUBLIC;
