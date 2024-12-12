import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


class NonTemplateSSHOperator(SSHOperator):
    template_fields = []


with DAG(
    dag_id="ssh_to_centerm_tripadvisor_pipeline",
    schedule="00 07 * * *",
    start_date=datetime.datetime(2024, 12, 12),
    tags=["ssh", "tripadvisor"],
) as dag:
    scrape_and_load_to_bq = NonTemplateSSHOperator(
        task_id="scrape_and_load_to_bq",
        ssh_conn_id="ssh.centerm.ubuntu.conn",
        command="cd /opt/apps/insight-engineers/tripadvisor-pipeline/tripadvisor && ./bin/execute.sh -n 100",
        cmd_timeout=None,
        conn_timeout=None,
        do_xcom_push=False,
    )

    # TODO: Add a task to dbt transformation

    scrape_and_load_to_bq
