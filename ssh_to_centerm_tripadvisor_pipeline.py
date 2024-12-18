import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup


class NonTemplateSSHOperator(SSHOperator):
    template_fields = []


with DAG(
    dag_id="ssh_to_centerm_tripadvisor_pipeline",
    schedule="0 0 * * 0",
    start_date=datetime.datetime(2024, 12, 17),
    tags=["ssh", "tripadvisor"],
) as dag:
    scrape_and_load_to_bq = NonTemplateSSHOperator(
        task_id="scrape_and_load_to_bq",
        ssh_conn_id="ssh.centerm.ubuntu.conn",
        command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/tripadvisor && ./bin/scrape.sh -n 200",
        cmd_timeout=None,
        conn_timeout=None,
        do_xcom_push=False,
    )

    with TaskGroup("dbt_tasks") as dbt_tasks:
        dbt_seed = NonTemplateSSHOperator(
            task_id="dbt_seed",
            ssh_conn_id="ssh.centerm.ubuntu.conn",
            command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt seed",
            cmd_timeout=None,
            conn_timeout=None,
            do_xcom_push=False,
        )

        with TaskGroup("dbt_staging") as dbt_staging:
            dbt_staging_base_geolocation = NonTemplateSSHOperator(
                task_id="dbt_staging_base_geolocation",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.staging.base.base_tripadvisor__geolocation",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

            dbt_staging_stg_location = NonTemplateSSHOperator(
                task_id="dbt_staging_stg_location",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.staging.stg_tripadvisor__location",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

            dbt_staging_stg_review = NonTemplateSSHOperator(
                task_id="dbt_staging_stg_review",
                ssh_conn_id="ssh.centerm.ubuntu.conn",
                command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/dbt-tripadvisor && dbt run --select dbt_tripadvisor.staging.stg_tripadvisor__review",
                cmd_timeout=None,
                conn_timeout=None,
                do_xcom_push=False,
            )

        dbt_seed >> dbt_staging

        dbt_staging_base_geolocation >> [
            dbt_staging_stg_location,
            dbt_staging_stg_review,
        ]

    scrape_and_load_to_bq >> dbt_tasks
