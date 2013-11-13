/*
 * Function to manage data present in the base tables to be partitioned.
 */
CREATE FUNCTION refresh_materialized_views() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_row                           record;

BEGIN

FOR v_row IN
SELECT schemaname, matviewname FROM pg_matviews
LOOP
    EXECUTE 'REFRESH MATERIALIZED VIEW '||v_row.schemaname||'.'||v_row.matviewname;
END LOOP; -- end of refresh loop

END
$$;
