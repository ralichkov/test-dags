import pendulum
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

class CustomKpo(KubernetesPodOperator):
    def on_kill(self) -> None:
        import traceback
        stack = traceback.extract_stack()
        self.log.warning("Operator killed.")

        caller = stack[-2]  # current function is last
        self.log.warning(
            f"caller -> func={caller.function}, file={caller.filename}, line={caller.lineno}"
        )

        grandcaller = stack[-3]
        self.log.warning(
            f"grandcaller -> func={grandcaller.function}, file={grandcaller.filename}, line={grandcaller.lineno}"
        )
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
