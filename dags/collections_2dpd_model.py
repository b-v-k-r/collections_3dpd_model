from datetime import datetime, timedelta

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG, Variable
from kubernetes.client import models as k8s_models

DAG_ID = "collections_2dpd_model"
config = Variable.get(DAG_ID, deserialize_json=True)

IMAGE = config["image_uri"]
NAMESPACE = "stage-dataplatform" if config["env"] == "dev" else "prod-dataplatform"
POD_PRIORITY = config["priority_class_name"]
NODE_SELECTOR = config["node_selectors"]
PREDICTION_OUTPUT_TABLE = config.get("prediction_output_table")
PREDICTION_FEATURES_TABLE = config.get("prediction_features_table")
PREDICTION_PARSER = config.get("prediction_parser", "pred")
PREDICTION_OUTPUT_CSV = config.get("prediction_output_csv")
PREDICTION_PRED_BASE_TABLE = config.get("prediction_pred_base_table")
PREDICTION_PRED_DAILY_TABLE = config.get("prediction_pred_daily_table")


args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 12),
    "email": ["data@khatabook.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=config.get("schedule_interval", "0 2 * * *"),
    max_active_runs=1,
    concurrency=4,
    catchup=False,
)

resource_config_default = {
    "KubernetesExecutor": {
        "request_memory": str(config["request_memory"]) + "Mi",
        "limit_memory": str(config["limit_memory"]) + "Mi",
        "request_cpu": config["request_cpu"],
        "limit_cpu": config["limit_cpu"],
        "node_selectors": NODE_SELECTOR,
    }
}

container_config_high = k8s_models.V1ResourceRequirements(
    limits={
        "memory": "16384Mi",
        "cpu": config["request_cpu"] * 2,
    },
    requests={
        "memory": "8192Mi",
        "cpu": config["request_cpu"],
    },
)


def _runtime_prefix() -> str:
    if config["env"] == "dev":
        return "./entrypoint-dev.sh && cd /app/collections_2dpd_model && "
    return "./entrypoint-prod.sh && cd /app && "


VAULT_ANNOTATIONS = {
    "vault.hashicorp.com/agent-inject": "true",
    "vault.hashicorp.com/role": config.get("vault_role", DAG_ID),
    "vault.hashicorp.com/agent-inject-secret-secrets.txt": config.get(
        "vault_secret_path", f"secret/data/{DAG_ID}"
    ),
    "vault.hashicorp.com/agent-inject-template-secrets.txt": config.get(
        "vault_secret_template",
        '{{- with secret "'
        + config.get("vault_secret_path", f"secret/data/{DAG_ID}")
        + '" -}}\n'
        "{{ range $k, $v := .Data.data }}{{ $k }}={{ $v }}\n{{ end }}\n"
        "{{- end }}",
    ),
}


SNOWFLAKE_ENV_VARS = {
    "ENV": config["env"],
    "SNOWFLAKE_USER": config.get("snowflake_user", ""),
    "SNOWFLAKE_PASSWORD": config.get("snowflake_password", ""),
    "SNOWFLAKE_ACCOUNT": config.get("snowflake_account", ""),
    "SNOWFLAKE_ROLE": config.get("snowflake_role", ""),
    "SNOWFLAKE_WAREHOUSE": config.get("snowflake_warehouse", ""),
    "SNOWFLAKE_DATABASE": config.get("snowflake_database", ""),
    "SNOWFLAKE_SCHEMA": config.get("snowflake_schema", ""),
}


def build_task(task_id, name, command, timeout_min=None, container_resources=None):
    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
        namespace=NAMESPACE,
        image=IMAGE,
        cmds=["bash", "-c"],
        arguments=[_runtime_prefix() + command],
        service_account_name="airflow-worker",
        env_vars=SNOWFLAKE_ENV_VARS,
        dag=dag,
        execution_timeout=timedelta(minutes=int(timeout_min or config["timeout_min"])),
        container_resources=container_resources or container_config_high,
        executor_config=resource_config_default,
        startup_timeout_seconds=600,
        get_logs=True,
        node_selector=NODE_SELECTOR,
        priority_class_name=POD_PRIORITY,
    )


test = build_task(
    "test",
    "run_test",
    'python3 -c "import db_service; import prediction.predict; print(\'imports ok\')"',
    timeout_min=10,
)

prediction_command_parts = [
    "python3 prediction/predict.py",
]
if PREDICTION_OUTPUT_TABLE:
    prediction_command_parts.append(f"--output-table {PREDICTION_OUTPUT_TABLE}")
if PREDICTION_FEATURES_TABLE:
    prediction_command_parts.append(f"--features-table {PREDICTION_FEATURES_TABLE}")
if PREDICTION_OUTPUT_CSV:
    prediction_command_parts.append(f"--output-csv {PREDICTION_OUTPUT_CSV}")
if PREDICTION_PRED_BASE_TABLE:
    prediction_command_parts.append(f"--pred-base-table {PREDICTION_PRED_BASE_TABLE}")
if PREDICTION_PRED_DAILY_TABLE:
    prediction_command_parts.append(f"--pred-daily-table {PREDICTION_PRED_DAILY_TABLE}")
if PREDICTION_PARSER:
    prediction_command_parts.append(f"--parser {PREDICTION_PARSER}")

prediction = build_task(
    "prediction",
    "run_prediction",
    " ".join(prediction_command_parts),
    timeout_min=config.get("prediction_timeout_min", config["timeout_min"]),
)


test >> prediction
