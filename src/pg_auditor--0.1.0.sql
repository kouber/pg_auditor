\echo Use "CREATE EXTENSION pg_auditor" to load this file. \quit

CREATE FUNCTION get_custom_data() RETURNS hstore AS $$
BEGIN
  RETURN NULL::hstore;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE TYPE operation AS ENUM ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE');


CREATE TABLE log (
  id bigserial PRIMARY KEY,
  relation_id oid,
  schema_name name NOT NULL,
  table_name name NOT NULL,
  operation @extschema@.operation NOT NULL,
  old_rec hstore,
  rec hstore,
  pg_user name NOT NULL DEFAULT CURRENT_USER,
  application_name text DEFAULT CURRENT_SETTING('application_name'),
  ip inet DEFAULT INET_CLIENT_ADDR(),
  process_id int DEFAULT PG_BACKEND_PID(),
  session_start timestamp,
  aux_data hstore DEFAULT @extschema@.get_custom_data(),
  transaction_id bigint NOT NULL DEFAULT TXID_CURRENT(),
  transaction_datetime timestamp NOT NULL DEFAULT TRANSACTION_TIMESTAMP(),
  clock_datetime timestamp NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

CREATE INDEX auditor_txid_idx ON log (transaction_id);
CREATE INDEX auditor_txdate_idx ON log (transaction_datetime);
CREATE INDEX auditor_process_idx ON log (process_id);

SELECT pg_catalog.pg_extension_config_dump('log', '');
SELECT pg_catalog.pg_extension_config_dump('log_id_seq', '');


CREATE TYPE operation_row AS (
  relation_id oid,
  operation @extschema@.operation,
  transaction_id bigint,
  rec hstore,
  old_rec hstore
);


CREATE FUNCTION logger() RETURNS trigger AS $$
DECLARE
  old_data hstore;
  new_data hstore;
  sess_start timestamp;
BEGIN
  IF (TG_OP = 'INSERT') THEN
    new_data := HSTORE(NEW.*);
  ELSIF (TG_OP = 'UPDATE') THEN
    new_data := HSTORE(NEW.*);
    old_data := HSTORE(OLD.*);
  ELSIF (TG_OP = 'DELETE') THEN
    old_data := HSTORE(OLD.*);
  END IF;

  SELECT INTO sess_start
    backend_start
  FROM
    pg_stat_activity
  WHERE
    pid = PG_BACKEND_PID();

  INSERT INTO @extschema@.log
    (relation_id, schema_name, table_name, operation, old_rec, rec, session_start)
  VALUES
    (TG_RELID, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP::@extschema@.operation, old_data, new_data, sess_start);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE FUNCTION get_primary_key(name, name) RETURNS SETOF information_schema.sql_identifier AS $$
SELECT
  column_name
FROM
  information_schema.constraint_column_usage AS ccu INNER JOIN information_schema.table_constraints AS tc
ON
  ccu.constraint_schema=tc.constraint_schema AND ccu.constraint_name=tc.constraint_name AND ccu.table_schema=tc.table_schema AND ccu.table_name=tc.table_name
WHERE
  tc.table_schema=$1
AND
  tc.table_name=$2
AND
  tc.constraint_type='PRIMARY KEY';
$$ LANGUAGE SQL STABLE STRICT;


CREATE FUNCTION cancel("row" @extschema@.operation_row) RETURNS boolean AS $$
DECLARE
  rel_name text;
  v_table_name name;
  v_schema_name name;
BEGIN
  IF row.operation = 'TRUNCATE' THEN
    RAISE EXCEPTION 'Truncate operation detected in transaction %', row.transaction_id;
  END IF;

  -- TODO: check for update of the PK itself

  SELECT INTO v_schema_name, v_table_name
    s.nspname,
    t.relname
  FROM
    pg_catalog.pg_class AS t INNER JOIN pg_catalog.pg_namespace AS s
  ON
    t.relnamespace = s.oid
  WHERE
    t.oid = row.relation_id;

  rel_name := FORMAT('%I.%I', v_schema_name, v_table_name);

  IF row.operation = 'INSERT' OR row.operation = 'UPDATE' THEN
    DECLARE
      where_clause TEXT;
    BEGIN
      EXECUTE
        'SELECT
           STRING_AGG(pk || '' = (NULL::'||rel_name||' #= $1).'' || pk, '' AND '')
        FROM
          @extschema@.get_primary_key($1, $2) AS pk'
      INTO
        where_clause
      USING
        v_schema_name, v_table_name;

      IF where_clause IS NULL THEN
        RAISE NOTICE 'No primary key found for relation %.% (action skipped)', v_schema_name, v_table_name;
        RETURN FALSE;
      END IF;

      IF row.operation = 'INSERT' THEN
        EXECUTE
          'DELETE FROM ' || rel_name || ' WHERE ' || where_clause
        USING
          row.rec;
      ELSE
        DECLARE
          list text;
          rec_list text;
        BEGIN
          SELECT INTO list, rec_list
            STRING_AGG(column_name, ','),
            STRING_AGG('(NULL::'||rel_name||' #= $1).'||column_name, ',')
          FROM
            information_schema.columns
          WHERE
            table_schema = v_schema_name
          AND
            table_name = v_table_name;

          EXECUTE
            'UPDATE ' || rel_name || ' SET ('||list||') = ('||rec_list||') WHERE ' || where_clause
          USING
            row.old_rec;
        END;
      END IF;
    END;
  ELSE
    EXECUTE
      'INSERT INTO ' || rel_name || ' SELECT (NULL::' || rel_name || ' #= $1).*'
    USING
      row.old_rec;
  END IF;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE FUNCTION cancel(bigint) RETURNS bigint AS $$
DECLARE
  row @extschema@.operation_row;
  v_rows_processed bigint default 0;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  FOR row IN
    SELECT
      relation_id,
      operation,
      transaction_id,
      rec,
      old_rec
    FROM
      @extschema@.log
    WHERE
      transaction_id = $1
    ORDER BY
      auditor_log_sid DESC
  LOOP
    PERFORM @extschema@.cancel(row);

    v_rows_processed := v_rows_processed + 1;
  END LOOP;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE FUNCTION flashback(bigint) RETURNS bigint AS $$
DECLARE
  row @extschema@.operation_row;
  v_rows_processed bigint default 0;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  FOR row IN
    SELECT
      relation_id,
      operation,
      transaction_id,
      rec,
      old_rec
    FROM
      @extschema@.log
    WHERE
      transaction_id > $1
    ORDER BY
      id DESC
  LOOP
    PERFORM @extschema@.cancel(row);

    v_rows_processed := v_rows_processed + 1;
  END LOOP;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE FUNCTION flashback(timestamp) RETURNS bigint AS $$
DECLARE
  v_txid bigint;
BEGIN
  IF $1 >= NOW() THEN
    RAISE EXCEPTION 'Travelling into the future is not allowed!';
  END IF;

  SELECT INTO v_txid
    MAX(transaction_id)
  FROM
    @extschema@.log
  WHERE
    transaction_datetime < $1;

  IF v_txid IS NULL THEN
    RAISE EXCEPTION 'Timestamp too far into the past.';
  ELSE
    RETURN @extschema@.flashback(v_txid);
  END IF;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE FUNCTION undo(steps bigint default 1, override_others boolean default false) RETURNS bigint AS $$
DECLARE
  v_txid bigint;
  row @extschema@.operation_row;
  v_rows_processed bigint default 0;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  steps := ABS(steps);

  IF steps = 0 THEN
    steps = 1;
  END IF;

  EXECUTE
    'SELECT
        DISTINCT transaction_id
      FROM
        @extschema@.log
      WHERE
        process_id = PG_BACKEND_PID()
      AND
        session_start = (
          SELECT
            backend_start
          FROM
            pg_stat_activity
          WHERE
            pid = PG_BACKEND_PID()
        )
      ORDER BY
        transaction_id DESC
      LIMIT
        1
      OFFSET
        $1'
  INTO
    v_txid
  USING
    $1 - 1;

  IF v_txid IS NULL THEN
    RAISE EXCEPTION 'There weren''t % DML transaction(s) yet in this session.', steps;
  END IF;

  IF override_others THEN
    FOR row IN
      SELECT
        relation_id,
        operation,
        transaction_id,
        rec,
        old_rec
      FROM
        @extschema@.log
      WHERE
        transaction_id >= v_txid
      ORDER BY
        id DESC
    LOOP
      PERFORM @extschema@.cancel(row);
      v_rows_processed := v_rows_processed + 1;
    END LOOP;
  ELSE
    FOR row IN
      SELECT
        relation_id,
        operation,
        transaction_id,
        rec,
        old_rec
      FROM
        @extschema@.log
      WHERE
        transaction_id >= v_txid
      AND
        process_id = PG_BACKEND_PID()
      AND
        session_start = (
          SELECT
            backend_start
          FROM
            pg_stat_activity
          WHERE
            pid = PG_BACKEND_PID()
        )
      ORDER BY
        id DESC
    LOOP
      PERFORM @extschema@.cancel(row);
      v_rows_processed := v_rows_processed + 1;
    END LOOP;
  END IF;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE FUNCTION is_array(anyelement) RETURNS BOOLEAN AS $$
  SELECT typelem <> '0' AND typarray = '0' FROM pg_type WHERE oid=pg_typeof($1);
$$ LANGUAGE sql IMMUTABLE STRICT;


CREATE TYPE evolution_row AS (
  transaction_id bigint,
  clock_datetime timestamp,
  operation @extschema@.operation,
  old text,
  new text
);


CREATE FUNCTION evolution(p_relname regclass, p_field_name name, pk_value anyelement) RETURNS SETOF @extschema@.evolution_row AS $$
DECLARE
  v_schema_name name;
  v_table_name name;
  pk_keys text[];
  pk_count int;
  query text DEFAULT 'SELECT
                        transaction_id,
                        clock_datetime,
                        operation,
                        old_rec->$3,
                        rec->$3
                      FROM
                        @extschema@.log
                      WHERE
                        schema_name = $1
                      AND
                        table_name = $2
                      AND
                        (old_rec->$4 = $5 OR rec->$4 = $5)
                      ORDER BY
                        clock_datetime';
BEGIN
  SELECT INTO v_schema_name, v_table_name
    schemaname,
    relname
  FROM
    pg_catalog.pg_statio_user_tables
  WHERE
    relid = $1;

  SELECT INTO pk_keys, pk_count
    ARRAY_AGG(pk::text),
    COUNT(pk)
  FROM
    @extschema@.get_primary_key(v_schema_name, v_table_name) AS pk;

  IF pk_count = 0 THEN
    RAISE EXCEPTION 'No primary key found for relation %.%', p_schema_name, p_table_name;
  ELSIF pk_count = 1 THEN
    IF @extschema@.is_array(pk_value) THEN
      RAISE EXCEPTION 'Relation %.% has a single primary key, scalar value expected for parameter #4', p_schema_name, p_table_name;
    END IF;

    RETURN QUERY EXECUTE
      query
    USING
      v_schema_name, v_table_name, p_field_name, pk_keys[1], pk_value::text;
  ELSE
    IF NOT @extschema@.is_array(pk_value) THEN
      RAISE EXCEPTION 'Relation %.% has a compound primary key, array expected for parameter #4', p_schema_name, p_table_name;
    END IF;

    RETURN QUERY EXECUTE
      query
    USING
      v_schema_name, v_table_name, p_field_name, pk_keys, pk_value;
  END IF;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE FUNCTION attach(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'CREATE TRIGGER
        auditor_logger
      AFTER INSERT OR UPDATE OR DELETE ON
        %I
      FOR EACH ROW EXECUTE PROCEDURE
        @extschema@.logger()', relname);

    EXECUTE FORMAT(
      'CREATE TRIGGER
        auditor_logger_truncate
      AFTER TRUNCATE ON
        %I
      FOR EACH STATEMENT EXECUTE PROCEDURE
        @extschema@.logger()', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION detach(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'DROP TRIGGER
        auditor_logger
      ON
        %I', relname);

    EXECUTE FORMAT(
      'DROP TRIGGER
        auditor_logger_truncate
      ON
        %I', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION auditor_abort_truncate() RETURNS trigger AS $$
  BEGIN
    RAISE EXCEPTION 'Truncate operation forbidden.';

    RETURN NULL;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE FUNCTION forbid_truncate(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'CREATE TRIGGER
        auditor_forbid_truncate
      BEFORE TRUNCATE ON
        %I
      FOR EACH STATEMENT EXECUTE PROCEDURE
        @extschema@.auditor_abort_truncate()', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION allow_truncate(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'DROP TRIGGER
        auditor_forbid_truncate
      ON
        %I', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
