CREATE OR REPLACE FUNCTION auditor.cancel(bigint) RETURNS bigint AS $$
DECLARE
  row auditor.operation_row;
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
      auditor.log
    WHERE
      transaction_id = $1
    ORDER BY
      id DESC
  LOOP
    PERFORM auditor.cancel(row);

    v_rows_processed := v_rows_processed + 1;
  END LOOP;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


ALTER EXTENSION pg_auditor DROP FUNCTION auditor.attach(regclass);

DROP FUNCTION auditor.attach(regclass);


CREATE OR REPLACE FUNCTION auditor.attach(relname regclass, variadic dml text[] default null) RETURNS boolean AS $$
DECLARE
  cmd text;
  sql text default 'INSERT OR UPDATE OR DELETE';
BEGIN
  BEGIN
    IF dml IS NULL OR 'TRUNCATE' = ANY(dml) OR 'truncate' = ANY(dml) THEN
      dml := ARRAY_REMOVE(dml, 'TRUNCATE');
      dml := ARRAY_REMOVE(dml, 'truncate');

      EXECUTE FORMAT(
        'CREATE TRIGGER
          auditor_logger_truncate
        AFTER TRUNCATE ON
          %I
        FOR EACH STATEMENT EXECUTE PROCEDURE
          auditor.logger()', relname);
    END IF;

    IF dml IS NOT NULL THEN
      FOREACH cmd IN ARRAY dml
      LOOP
        IF NOT UPPER(cmd) = ANY ('{INSERT,UPDATE,DELETE,TRUNCATE}'::text[]) THEN
          RAISE EXCEPTION '% is not a valid DML command', cmd;
        END IF;
      END LOOP;

      sql = ARRAY_TO_STRING(dml, ' OR ');
    END IF;

    EXECUTE FORMAT(
      'CREATE TRIGGER
        auditor_logger
      AFTER %s ON
        %I
      FOR EACH ROW EXECUTE PROCEDURE
        auditor.logger()', sql, relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION auditor.detach(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'DROP TRIGGER
        auditor_logger
      ON
        %I', relname);

    EXECUTE FORMAT(
      'DROP TRIGGER IF EXISTS
        auditor_logger_truncate
      ON
        %I', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


ALTER TABLE auditor.log RENAME rec TO new_rec;

ALTER TYPE auditor.operation_row RENAME ATTRIBUTE rec TO new_rec;


CREATE OR REPLACE FUNCTION auditor.logger() RETURNS trigger AS $$
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

  INSERT INTO auditor.log
    (relation_id, schema_name, table_name, operation, old_rec, new_rec, session_start)
  VALUES
    (TG_RELID, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP::auditor.operation, old_data, new_data, sess_start);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE OR REPLACE FUNCTION auditor.cancel("row" auditor.operation_row) RETURNS boolean AS $$
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
          auditor.get_primary_key($1, $2) AS pk'
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
          row.new_rec;
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


CREATE OR REPLACE FUNCTION auditor.cancel(bigint) RETURNS bigint AS $$
DECLARE
  row auditor.operation_row;
  v_rows_processed bigint default 0;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  FOR row IN
    SELECT
      relation_id,
      operation,
      transaction_id,
      new_rec,
      old_rec
    FROM
      auditor.log
    WHERE
      transaction_id = $1
    ORDER BY
      id DESC
  LOOP
    PERFORM auditor.cancel(row);

    v_rows_processed := v_rows_processed + 1;
  END LOOP;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION auditor.flashback(bigint) RETURNS bigint AS $$
DECLARE
  row auditor.operation_row;
  v_rows_processed bigint default 0;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  FOR row IN
    SELECT
      relation_id,
      operation,
      transaction_id,
      new_rec,
      old_rec
    FROM
      auditor.log
    WHERE
      transaction_id > $1
    ORDER BY
      id DESC
  LOOP
    PERFORM auditor.cancel(row);

    v_rows_processed := v_rows_processed + 1;
  END LOOP;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION auditor.undo(steps bigint default 1, override_others boolean default false) RETURNS bigint AS $$
DECLARE
  v_txid bigint;
  row auditor.operation_row;
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
        auditor.log
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
        new_rec,
        old_rec
      FROM
        auditor.log
      WHERE
        transaction_id >= v_txid
      ORDER BY
        id DESC
    LOOP
      PERFORM auditor.cancel(row);
      v_rows_processed := v_rows_processed + 1;
    END LOOP;
  ELSE
    FOR row IN
      SELECT
        relation_id,
        operation,
        transaction_id,
        new_rec,
        old_rec
      FROM
        auditor.log
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
      PERFORM auditor.cancel(row);
      v_rows_processed := v_rows_processed + 1;
    END LOOP;
  END IF;

  RETURN v_rows_processed;
END;
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION auditor.evolution(p_relname regclass, p_field_name name, pk_value anyelement) RETURNS SETOF auditor.evolution_row AS $$
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
                        new_rec->$3
                      FROM
                        auditor.log
                      WHERE
                        schema_name = $1
                      AND
                        table_name = $2
                      AND
                        (old_rec->$4 = $5 OR new_rec->$4 = $5)
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
    auditor.get_primary_key(v_schema_name, v_table_name) AS pk;

  IF pk_count = 0 THEN
    RAISE EXCEPTION 'No primary key found for relation %.%', p_schema_name, p_table_name;
  ELSIF pk_count = 1 THEN
    IF auditor.is_array(pk_value) THEN
      RAISE EXCEPTION 'Relation %.% has a single primary key, scalar value expected for parameter #4', p_schema_name, p_table_name;
    END IF;

    RETURN QUERY EXECUTE
      query
    USING
      v_schema_name, v_table_name, p_field_name, pk_keys[1], pk_value::text;
  ELSE
    IF NOT auditor.is_array(pk_value) THEN
      RAISE EXCEPTION 'Relation %.% has a compound primary key, array expected for parameter #4', p_schema_name, p_table_name;
    END IF;

    RETURN QUERY EXECUTE
      query
    USING
      v_schema_name, v_table_name, p_field_name, pk_keys, pk_value;
  END IF;
END;
$$ LANGUAGE plpgsql STRICT;
