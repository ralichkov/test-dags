import pendulum
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

class CustomKpo(KubernetesPodOperator):
    def on_kill(self) -> None:
        print("Operator got killed.")
        self.log.warning("Operator killed.")
        return

with DAG(
    dag_id="kpo",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule=None,
    default_args={"retries": 0},
):
    CustomKpo(
        task_id="idle",
        name="idle",
        namespace="airflow",
        image="bash:5.2",
        cmds=["bash", "-c"],
        arguments=["for i in $(seq 1 600); do echo tick $i; sleep 1; done"],
        in_cluster=True,
        on_finish_action="delete_pod",
        reattach_on_restart=True,
        termination_grace_period=60,
        termination_message_policy="FallbackToLogsOnError",
    )
