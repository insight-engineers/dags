import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup


class CentermSSHOperator(SSHOperator):
    template_fields = []


with DAG(
    dag_id="ssh_to_centerm_tripadvisor_pipeline",
    schedule="0 0 * * 0",
    start_date=datetime.datetime(2024, 12, 17),
    tags=["ssh", "tripadvisor"],
) as dag:
    scrape_and_load_to_bq = CentermSSHOperator(
        task_id="scrape_and_load_to_bq",
        ssh_conn_id="ssh.centerm.ubuntu.conn",
        command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/tripadvisor && ./bin/scrape.sh -n 0",
        cmd_timeout=None,
        conn_timeout=None,
        do_xcom_push=False,
    )

    with TaskGroup("dbt_tasks") as dbt_tasks:
        dbt_seed = CentermSSHOperator(
            task_id="dbt_seed",
            ssh_conn_id="ssh.centerm.ubuntu.conn",
            command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt seed",
            cmd_timeout=None,
            conn_timeout=None,
            do_xcom_push=False,
        )

        with TaskGroup("dbt_staging") as dbt_staging:
            dbt_staging_base_geolocation = CentermSSHOperator(
                task_id="dbt_staging_base_geolocation",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.staging.base.base_tripadvisor__geolocation",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

            dbt_staging_stg_location = CentermSSHOperator(
                task_id="dbt_staging_stg_location",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.staging.stg_tripadvisor__location",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

            dbt_staging_stg_review = CentermSSHOperator(
                task_id="dbt_staging_stg_review",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.staging.stg_tripadvisor__review",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )
            
        with TaskGroup("dbt_intermediate") as dbt_intermediate:
            dbt_intermediate_int_location = CentermSSHOperator(
                task_id="dbt_intermediate_int_location",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.intermediate.intermediate_tripadvisor__location",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

            dbt_intermediate_int_review = CentermSSHOperator(
                task_id="dbt_intermediate_int_review",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.intermediate.intermediate_tripadvisor__review",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

        with TaskGroup("dbt_mart") as dbt_mart:
            dbt_mart_dim_cuisine = CentermSSHOperator(
                task_id="dbt_mart_dim_cuisine",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.marts.dim_cuisine",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

            dbt_mart_dim_location = CentermSSHOperator(
                task_id="dbt_mart_dim_location",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.marts.dim_location",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )
            
            dbt_mart_dim_review = CentermSSHOperator( 
                task_id="dbt_mart_dim_review",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.marts.dim_review",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )
            
            dbt_mart_dim_user = CentermSSHOperator(
                task_id="dbt_mart_dim_user",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.marts.dim_user",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )
            
            dbt_mart_fact_restaurant_feedback = CentermSSHOperator( 
                task_id="dbt_mart_fact_restaurant_feedback",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.marts.fact_restaurant_feedback",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )
        
        dbt_seed >> dbt_staging >> dbt_intermediate >> dbt_mart
       
        dbt_staging_base_geolocation >> [
            dbt_staging_stg_location,
            dbt_staging_stg_review,
        ]

    scrape_and_load_to_bq >> dbt_tasks