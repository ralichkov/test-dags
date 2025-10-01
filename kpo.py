import pendulum
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

def create_custom_kpo():
    kpo = KubernetesPodOperator(
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

    def kill_override(self):
        print("Operator got killed.")
        self.log.warning("Operator killed.")
        return

    kpo.on_kill = kill_override

    return kpo


with DAG(
    dag_id="kpo",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule=None,
    default_args={"retries": 0},
):
    op = create_custom_kpo()
