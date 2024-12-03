drop table if exists {schema}.on_model_hook;

create table {schema}.on_model_hook (
    test_state       string, -- start|end
    target_dbname    string,
    target_host      string,
    target_name      string,
    target_schema    string,
    target_type      string,
    target_user      string,
    target_pass      string,
    target_threads   int,
    run_started_at   string,
    invocation_id    string,
    thread_id        string
);
