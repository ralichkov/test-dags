import pendulum
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="kpo",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule=None,
    default_args={"retries": 0},
):
    KubernetesPodOperator(
        task_id="idle",
        name="idle",
        namespace="airflow",
        image="bash:5.2",
        cmds=["bash", "-c"],
        arguments=["for i in $(seq 1 600); do echo tick $i; sleep 1; done"],
        in_cluster=True,
        on_finish_action="delete_pod",
    )
