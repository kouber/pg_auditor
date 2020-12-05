CREATE OR REPLACE FUNCTION auditor.get_custom_data() RETURNS hstore AS $$
BEGIN
  -- RETURN HSTORE('username', get_session_var('USERNAME'));
  RETURN NULL::hstore;
END;
$$ LANGUAGE plpgsql VOLATILE;