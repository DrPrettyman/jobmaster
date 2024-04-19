import sqlalchemy


def deploy(db_engine: sqlalchemy.engine.base.Engine, schema: str, reset: bool = False):
    if reset:
        _kill(db_engine, schema)
    _create_schema(db_engine, schema)
    _create_tables(db_engine, schema)
    _create_functions(db_engine, schema)


def _create_schema(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    with db_engine.connect() as conn:
        conn.execute(sqlalchemy.schema.CreateSchema(schema, if_not_exists=True))
        conn.commit()


def _create_tables(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    with db_engine.connect() as conn:

        # Task Definitions
        conn.execute(
            sqlalchemy.text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.task_types (
                    type_key        text NOT NULL,
                    help            text
                );
                
                CREATE TABLE IF NOT EXISTS {schema}.tasks (
                    type_key        text NOT NULL,
                    task_key        text NOT NULL,
                    help            text,
                    process_limit   integer,
                    write_all       text ARRAY
                );
                
                CREATE TABLE IF NOT EXISTS {schema}.parameters (
                    type_key        text NOT NULL,
                    task_key        text NOT NULL,
                    parameter_key   text NOT NULL,
                    value_type      text,
                    select_from     json ARRAY,
                    default_value   json,
                    required        boolean DEFAULT false,
                    write_all       boolean DEFAULT false,
                    help            text DEFAULT 'No documentation available'
                );
                """
            )
        )

        # Task Dependency Definitions
        conn.execute(
            sqlalchemy.text(
                f"""
                        CREATE TABLE IF NOT EXISTS {schema}.dependencies (
                            type_key                text NOT NULL,
                            task_key                text NOT NULL,
                            dependency_type_key     text NOT NULL,
                            dependency_task_key     text NOT NULL,
                            time                    integer
                        );
                        
                        CREATE TABLE IF NOT EXISTS {schema}.dependency_args_same (
                            type_key                text NOT NULL,
                            task_key                text NOT NULL,
                            dependency_type_key     text NOT NULL,
                            dependency_task_key     text NOT NULL,
                            argument                text NOT NULL
                        );
                        
                        CREATE TABLE IF NOT EXISTS {schema}.dependency_args_specified (
                            type_key                text NOT NULL,
                            task_key                text NOT NULL,
                            dependency_type_key     text NOT NULL,
                            dependency_task_key     text NOT NULL,
                            argument                text NOT NULL,
                            arg_value               json NOT NULL
                        );
                """
            )
        )

        # Jobs queue
        conn.execute(
            sqlalchemy.text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.jobs (
                    job_id              uuid NOT NULL,
                    collect_id          uuid,
                    status              smallint DEFAULT 0 NOT NULL,
                    system_node_name    text DEFAULT 'external',
                    system_pid          integer DEFAULT 0,
                    cmd_id              text NOT NULL,
                    updated_at          timestamp DEFAULT now(),
                    priority            integer DEFAULT 0,
                    type_key            text NOT NULL,
                    task_key            text NOT NULL,
                    message             text
                );
                """
            )
        )

        # Arguments
        conn.execute(
            sqlalchemy.text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.arguments (
                    job_id      uuid NOT NULL,
                    arg_key     text,
                    arg_value   json
                );
                
                CREATE TYPE jobmaster_argument AS (
                    arg_key     text,
                    arg_value   json
                );
                """
            )
        )

        # Collect Ids
        conn.execute(
            sqlalchemy.text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.collect_ids (
                    collect_id          uuid NOT NULL,
                    time_added          timestamp DEFAULT now()
                );
                """
            )
        )

        conn.commit()


def _create_functions(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    with db_engine.connect() as conn:
        # table functions
        conn.execute(
            sqlalchemy.text(
                f"""    
                CREATE OR REPLACE {schema}.current_jobs () 
                RETURNS TABLE (
                    job_id_out uuid,
                    type_key_out text,
                    task_key_out text,
                    status_out smallint,
                    priority_out integer,
                    created_at_out timestamp,
                    updated_at_out timestamp,
                    arguments_out jobmaster_argument ARRAY
                ) 
                language plpgsql
                AS $$
                BEGIN
                    RETURN QUERY
                        WITH most_recent as (
                            SELECT      job_id, max(updated_at) as updated_at, min(updated_at) as created_at
                            FROM        {schema}.jobs
                            GROUP BY    job_id
                        ), all_jobs AS (
                            SELECT  most_recent.job_id,
                                    j1.type_key,
                                    j1.task_key,
                                    j1.status,
                                    j1.priority,
                                    most_recent.created_at,
                                    most_recent.updated_at
                            FROM    {schema}.jobs j1
                            RIGHT JOIN most_recent
                            ON  most_recent.job_id = j1.job_id and most_recent.updated_at = j1.updated_at
                        ), 
                        args1 AS (
                            SELECT  all_jobs.job_id,
                                    array_agg(row(args_table.arg_key, args_table.arg_value)::jobmaster_argument) as args
                            FROM  all_jobs
                            LEFT JOIN {schema}.arguments args_table
                            ON all_jobs.job_id = args_table.job_id
                            GROUP BY all_jobs.job_id
                        )
    
                        SELECT  all_jobs.job_id,
                                all_jobs.type_key,
                                all_jobs.task_key,
                                all_jobs.status,
                                all_jobs.priority,
                                all_jobs.created_at,
                                all_jobs.updated_at,
                                args1.args
                        FROM    all_jobs
                        LEFT JOIN args1
                        ON      all_jobs.job_id = args1.job_id
                        ;
                END;
                $$;
                """
            )
        )

        conn.commit()


def _kill(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    with db_engine.connect() as conn:
        conn.execute(sqlalchemy.text(f"DROP TYPE IF EXISTS argument;"))
        conn.execute(sqlalchemy.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE;"))
        conn.commit()


