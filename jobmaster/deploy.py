import sqlalchemy
from pg8000.exceptions import DatabaseError


def deploy(db_engine: sqlalchemy.engine.base.Engine, schema: str, reset: bool = False):
    if reset:
        _kill(db_engine, schema)
    _create_schema(db_engine, schema)
    _create_types(db_engine, schema)
    _create_tables(db_engine, schema)
    _create_functions(db_engine, schema)


def _create_schema(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    with db_engine.connect() as conn:
        conn.execute(sqlalchemy.schema.CreateSchema(schema, if_not_exists=True))
        conn.commit()


def _create_types(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    # Arguments
    try:
        with db_engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"""
                    CREATE TYPE jobmaster_argument AS (
                        arg_key     text,
                        arg_value   json
                    );
                    """
                )
            )
            conn.commit()
    except DatabaseError as e:
        pass


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
                    process_units   integer,
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
    current_jobs_function_sql = """
    CREATE OR REPLACE FUNCTION @SCHEMA_NAME.current_jobs () 
                RETURNS TABLE (
                    job_id_out uuid,
                    type_key_out text,
                    task_key_out text,
                    status_out smallint,
                    priority_out integer,
                    created_at_out timestamp,
                    updated_at_out timestamp,
                    arguments_out jobmaster_argument ARRAY,
                    system_node_name_out text,
                    system_pid_out integer,
                    cmd_id_out text
                ) 
                language plpgsql
                AS $$
                BEGIN
                    RETURN QUERY
                        WITH most_recent as (
                            SELECT      job_id, max(updated_at) as updated_at, min(updated_at) as created_at
                            FROM        @SCHEMA_NAME.jobs
                            GROUP BY    job_id
                        ), all_jobs AS (
                            SELECT  most_recent.job_id,
                                    j1.type_key,
                                    j1.task_key,
                                    j1.status,
                                    j1.priority,
                                    j1.system_node_name,
                                    j1.system_pid,
                                    j1.cmd_id,
                                    most_recent.created_at,
                                    most_recent.updated_at
                            FROM    @SCHEMA_NAME.jobs j1
                            RIGHT JOIN most_recent
                            ON  most_recent.job_id = j1.job_id and most_recent.updated_at = j1.updated_at
                        ), 
                        args1 AS (
                            SELECT  all_jobs.job_id,
                                    array_agg(row(args_table.arg_key, args_table.arg_value)::jobmaster_argument) as args
                            FROM  all_jobs
                            LEFT JOIN @SCHEMA_NAME.arguments args_table
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
                                args1.args,
                                all_jobs.system_node_name,
                                all_jobs.system_pid,
                                all_jobs.cmd_id
                        FROM    all_jobs
                        LEFT JOIN args1
                        ON      all_jobs.job_id = args1.job_id
                        ;
                END;
                $$;
    """.replace("@SCHEMA_NAME", schema)
    insert_procedure_sql = """
create or replace procedure @SCHEMA_NAME.insert(
    type_key_in text,
    task_key_in text,
    arguments_in json,
    priority_in integer default 1,
    system_name_in text default 'external',
    new_job_id inout uuid default null,
    message_out inout text default null
)
language plpgsql    
as $$
declare 
    conflicting_jobs uuid[];
    missing_arguments text[];
begin
    -- if no job_id was provided, generate one
    select coalesce(new_job_id, gen_random_uuid()) into new_job_id;
    
    -- check if the job is already in the queue
    if new_job_id in (select distinct job_id from @SCHEMA_NAME.jobs)
    then 
        select 'job already in queue' into message_out;
        return;
    end if;
    
    -- check the task exists
    if (select count(*) from @SCHEMA_NAME.tasks where type_key = type_key_in and task_key = task_key_in) = 0
    then
        select 'Invalid task: ' || type_key_in || '.' || task_key_in into message_out;
        select null into new_job_id;
        return;
    end if;
    
    -- check that the user provided the correct arguments
    with 
    -- create argsin table with the input data
    argsin as (
        select key as arg_key, value as arg_value, TRUE as yes from json_each(arguments_in)
    ), 
    -- select parameter_key from tasks matching the type_key and task_key
    t1 as (
        select parameter_key
        from @SCHEMA_NAME.parameters p
        where p.type_key = type_key_in and p.task_key = task_key_in and p.required = TRUE
    ), kvpairs as (
        select  t1.parameter_key,
                argsin.yes
        from t1 
        left join argsin
        on t1.parameter_key = argsin.arg_key
    )
    select array_agg(parameter_key)
    from kvpairs
    where yes is null
    into missing_arguments;
    
    if array_length(missing_arguments, 1) > 0
    then 
        select 'Missing arguments: ' || array_to_string(missing_arguments, ', ') into message_out;
        select null into new_job_id;
        return;
    end if;
    
    lock table @SCHEMA_NAME.jobs in ROW EXCLUSIVE mode;
    
    -- check if a job with the same args is already waiting/running
    with 
    -- create argsin and taskin: tables with the input data
    argsin as (
        select key as arg_key, value as arg_value from json_each(arguments_in)
    ), taskin as (
        select  type_key_in as type_key, task_key_in as task_key
    ), 
    -- get all jobs with current status 1 or 2
    wr_jobs as (
        select j.job_id, j.type_key, j.task_key 
        from @SCHEMA_NAME.jobs j
        right join (select job_id, max(updated_at) as updated_at from @SCHEMA_NAME.jobs group by job_id) k
        on j.job_id = k.job_id and j.updated_at = k.updated_at
        where j.status in (1, 2)
    ), 
    -- get jobs with the same type_key and task_key as the input
    task_confilicts as (
        select  wr_jobs.job_id
        from    taskin
        inner join wr_jobs
        on taskin.type_key = wr_jobs.type_key and taskin.task_key = wr_jobs.task_key
    ), 
    -- get the arguments of those jobs
    args_of_task_confilicts as (
        select      a.job_id, a.arg_key, a.arg_value
        from        task_confilicts
        left join   @SCHEMA_NAME.arguments a
        on task_confilicts.job_id = a.job_id
    ), 
    -- join with argsin on the key and value, then count the rows
    arg_conflicts as (
        select  a.job_id, count(*) as arg_count
        from    argsin
        inner join args_of_task_confilicts a
        on argsin.arg_key = a.arg_key and argsin.arg_value::jsonb = a.arg_value::jsonb
        group by a.job_id
    ), 
    -- if any job has the same number of matching args as there are input args, we know it has all the same args
    conflicts as (
        select a.job_id
        from   arg_conflicts a
        where  a.arg_count >= (select count(*) from argsin)
    )
    -- finally select the ids of conflicting jobs into an array
    select array_agg(job_id) from conflicts into conflicting_jobs;
    
    -- if there are any conflicting jobs, we return
    if array_length(conflicting_jobs, 1) > 0
    then 
        select 'Job with same specification already in queue. Returning job_id for that job.' into message_out;
        select conflicting_jobs[1] into new_job_id;
        return;
    end if;
    
    insert into @SCHEMA_NAME.jobs
    values (new_job_id, null, 1, system_name_in, 0, 'external', now(), priority_in, type_key_in, task_key_in, 'Inserted using insert_job procedure');
    
    insert into @SCHEMA_NAME.arguments
    select  new_job_id as job_id,
            key        as arg_key,
            value      as arg_value 
    from json_each(arguments_in);
    
    --commit;
    
    select 'inserted job' into message_out;
    
end;$$;
""".replace("@SCHEMA_NAME", schema)

    with db_engine.connect() as conn:
        # table functions
        conn.execute(sqlalchemy.text(current_jobs_function_sql))
        conn.execute(sqlalchemy.text(insert_procedure_sql))
        conn.commit()


def _kill(db_engine: sqlalchemy.engine.base.Engine, schema: str):
    with db_engine.connect() as conn:
        conn.execute(sqlalchemy.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE;"))
        conn.execute(sqlalchemy.text(f"DROP TYPE IF EXISTS jobmaster_argument CASCADE;"))
        conn.commit()


