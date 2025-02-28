ALTER TABLE @extschema@.log ALTER COLUMN pg_user SET DEFAULT SESSION_USER;


CREATE OR REPLACE FUNCTION @extschema@.attach(relname regclass, variadic dml text[] default null) RETURNS boolean AS $$
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
          %s
        FOR EACH STATEMENT EXECUTE PROCEDURE
          @extschema@.logger()', relname);
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
        %s
      FOR EACH ROW EXECUTE PROCEDURE
        @extschema@.logger()', sql, relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.detach(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'DROP TRIGGER
        auditor_logger
      ON
        %s', relname);

    EXECUTE FORMAT(
      'DROP TRIGGER IF EXISTS
        auditor_logger_truncate
      ON
        %s', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.forbid_truncate(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'CREATE TRIGGER
        auditor_forbid_truncate
      BEFORE TRUNCATE ON
        %s
      FOR EACH STATEMENT EXECUTE PROCEDURE
        @extschema@.auditor_abort_truncate()', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.allow_truncate(relname regclass) RETURNS boolean AS $$
BEGIN
  BEGIN
    EXECUTE FORMAT(
      'DROP TRIGGER
        auditor_forbid_truncate
      ON
        %s', relname);

  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '%', SQLERRM;
  END;

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
