/*
 * Function to manage data present in the base tables to be partitioned.
 */
CREATE FUNCTION partition_data(multiplier int DEFAULT 1) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock                      boolean;
v_job_id                        bigint;
v_jobmon_schema                 text;
v_old_search_path               text;
v_step_id                       bigint;
v_row                           record;

BEGIN

v_adv_lock := pg_try_advisory_lock(hashtext('pg_partman partition_data'));
IF v_adv_lock = 'false' THEN
    RAISE NOTICE 'Partman maintenance already running.';
    RETURN;
END IF;

SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF v_jobmon_schema IS NOT NULL THEN
    SELECT current_setting('search_path') INTO v_old_search_path;
    EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||''',''false'')';
END IF;

IF v_jobmon_schema IS NOT NULL THEN
    v_job_id := add_job('PARTMAN PARTION DATA');
    v_step_id := add_step(v_job_id, 'Partitioning data loop');
END IF;

FOR v_row IN 
SELECT parent_table
    , type
    , part_interval::interval
    , control
    , premake
    , datetime_string
    , last_partition
    , undo_in_progress
FROM @extschema@.part_config WHERE type = 'time-static' OR type = 'time-dynamic'
LOOP

    CONTINUE WHEN v_row.undo_in_progress;
    EXECUTE 'SELECT @extschema@.partition_data_time('||quote_literal(v_row.parent_table)||', '||v_row.premake*multiplier||')';
END LOOP; -- end of partition time loop

FOR v_row IN
SELECT parent_table
    , type
    , part_interval::interval
    , control
    , premake
    , datetime_string
    , last_partition
    , undo_in_progress
FROM @extschema@.part_config WHERE type = 'id-static' OR type = 'id-dynamic'
LOOP

    CONTINUE WHEN v_row.undo_in_progress;
    EXECUTE 'SELECT @extschema@.partition_data_id('||quote_literal(v_row.parent_table)||', '||v_row.premake*multiplier||')';
END LOOP; -- end of partition time loop

IF v_jobmon_schema IS NOT NULL THEN
    PERFORM update_step(v_step_id, 'OK', 'Partitioning data finished.');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
END IF;

PERFORM pg_advisory_unlock(hashtext('pg_partman partition_data'));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('pg_partman partition_data'));
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        IF v_jobmon_schema IS NOT NULL THEN
            EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||''',''false'')';
            IF v_job_id IS NULL THEN
                v_job_id := add_job('PARTMAN PARTITION DATA');
                v_step_id := add_step(v_job_id, 'EXCEPTION before job logging started');
            END IF;
            IF v_step_id IS NULL THEN
                v_step_id := add_step(v_job_id, 'EXCEPTION before first step logged');
            END IF;
            PERFORM update_step(v_step_id, 'CRITICAL', 'ERROR: '||coalesce(SQLERRM,'unknown'));
            PERFORM fail_job(v_job_id);
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('pg_partman partition_data'));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
