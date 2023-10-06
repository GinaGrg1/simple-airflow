from airflow.utils.dates import days_ago

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from pprint import pprint as pp

default_args = {
    'owner': 'rgurung'
}

@dag(
    dag_id = 'first_dag_with_taskapi',
    description = 'Creating the second dag',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['beginner', 'kwargs', 'second dag']
)
def first_dag_taskapi():

    @task
    def first_dag(**kwargs):
        pp(kwargs.keys())
    
    @task
    def second_dag(**kwargs):
        ti = kwargs.get('ti')
        pp(dir(ti))
    
    first_dag = first_dag()
    second_dag = second_dag()
    
    chain(
    first_dag,
    second_dag
    )

first_dag_taskapi()
######################################################################################

['are_dependencies_met',
 'are_dependents_done',
 'check_and_change_state_before_execution',
 'clear_db_references',
 'clear_next_method_args',
 'clear_xcom_data',
 'command_as_list',
 'current_state',
 'dag_id',
 'dag_model',
 'dag_run',
 'dry_run',
 'duration',
 'email_alert',
 'end_date',
 'error',
 'execution_date',
 'executor_config',
 'external_executor_id',
 'filter_for_tis',
 'generate_command',
 'get_dagrun',
 'get_email_subject_content',
 'get_failed_dep_statuses',
 'get_num_running_task_instances',
 'get_previous_dagrun',
 'get_previous_execution_date',
 'get_previous_start_date',
 'get_previous_ti',
 'get_relevant_upstream_map_indexes',
 'get_rendered_k8s_spec',
 'get_rendered_template_fields',
 'get_template_context',
 'get_truncated_error_traceback',
 'handle_failure',
 'hostname',
 'init_on_load',
 'init_run_context',
 'insert_mapping',
 'is_eligible_to_retry',
 'is_premature',
 'job_id',
 'key',
 'log',
 'log_url',
 'map_index',
 'mark_success_url',
 'max_tries',
 'metadata',
 'next_kwargs',
 'next_method',
 'next_retry_datetime',
 'next_try_number',
 'note',
 'operator',
 'overwrite_params_with_dag_run_conf',
 'pid',
 'pool',
 'pool_slots',
 'prev_attempted_tries',
 'previous_start_date_success',
 'previous_ti',
 'previous_ti_success',
 'priority_weight',
 'queue',
 'queued_by_job',
 'queued_by_job_id',
 'queued_dttm',
 'raw',
 'ready_for_retry',
 'refresh_from_db',
 'refresh_from_task',
 'registry',
 'render_k8s_pod_yaml',
 'render_templates',
 'rendered_task_instance_fields',
 'run',
 'run_as_user',
 'run_id',
 'schedule_downstream_tasks',
 'set_duration',
 'set_state',
 'start_date',
 'state',
 'task',
 'task_id',
 'task_instance_note',
 'test_mode',
 'ti_selector_condition',
 'trigger',
 'trigger_id',
 'trigger_timeout',
 'triggerer_job',
 'try_number',
 'unixname',
 'updated_at',
 'xcom_pull',
 'xcom_push']
