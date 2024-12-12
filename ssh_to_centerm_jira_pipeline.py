import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


class NonTemplateSSHOperator(SSHOperator):
    template_fields = []


with DAG(
    dag_id="ssh_to_centerm_jira_pipeline",
    schedule="00 00 * * *",
    start_date=datetime.datetime(2024, 12, 12),
    tags=["ssh", "jira"],
) as dag:
    ssh_to_centerm = NonTemplateSSHOperator(
        task_id="ssh_to_centerm_jira_pipeline",
        ssh_conn_id="ssh.centerm.ubuntu.conn",
        command="cd /opt/apps/insight-engineers/jira-pipeline && bash execute.sh",
        cmd_timeout=1200,
        conn_timeout=1200,
        do_xcom_push=False,
    )
    ssh_to_centerm
