from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.operators.bash import BashOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="daily_pipeline",
    description="A daily pipeline to run the producers, the processing and the dbt models",
    default_args=default_args,
    start_date=datetime(2026, 3, 27),
    schedule="0 0 * * *",
    catchup=False,
    tags=["energy", "weather"],
) as dag:
    run_weather_producer = DockerOperator(
        task_id="run_weather_producer",
        image="energy-pipeline-ingestion:latest",
        command="python -m producers.weather_producer",
        network_mode="energy-pipeline_pipeline_net",
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "ENTSOE_API_KEY": "{{ var.value.entsoe_api_key }}",
        },
        auto_remove=True,       # remove container after it exits
        docker_url="unix://var/run/docker.sock",
    )

    run_energy_producer = DockerOperator(
        task_id="run_energy_producer",
        image="energy-pipeline-ingestion:latest",
        command="python -m producers.energy_producer",
        network_mode="energy-pipeline_pipeline_net",
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "ENTSOE_API_KEY": "{{ var.value.entsoe_api_key }}",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    wait_for_weather_bronze = WasbBlobSensor(
    task_id="wait_for_weather_bronze",
    wasb_conn_id="azure_blob_storage",
    container_name="bronze",
    blob_name="weather/year={{ macros.ds_format(macros.ds_add(ds, -5), '%Y-%m-%d', '%Y') }}/month={{ macros.ds_format(macros.ds_add(ds, -5), '%Y-%m-%d', '%m') }}/day={{ macros.ds_format(macros.ds_add(ds, -5), '%Y-%m-%d', '%d') }}/",
    timeout=60 * 30,
    poke_interval=60,
    mode="poke",
)

    wait_for_energy_bronze = WasbBlobSensor(
        task_id="wait_for_energy_bronze",
        wasb_conn_id="azure_blob_storage",
        container_name="bronze",
        blob_name="energy/year={{ macros.ds_format(macros.ds_add(ds, -5), '%Y-%m-%d', '%Y') }}/month={{ macros.ds_format(macros.ds_add(ds, -5), '%Y-%m-%d', '%m') }}/day={{ macros.ds_format(macros.ds_add(ds, -5), '%Y-%m-%d', '%d') }}/",
        timeout=60 * 30,
        poke_interval=60,
        mode="poke",
    )

    run_silver_weather = DockerOperator(
        task_id="run_silver_weather",
        image="energy-pipeline-spark-jobs:latest",
        command="python3 -m jobs.silver_weather",
        network_mode="energy-pipeline_pipeline_net",
        environment={
            "AZURE_STORAGE_ACCOUNT_NAME": "{{ var.value.azure_storage_account_name }}",
            "AZURE_STORAGE_ACCOUNT_KEY": "{{ var.value.azure_storage_account_key }}",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    run_silver_energy = DockerOperator(
        task_id="run_silver_energy",
        image="energy-pipeline-spark-jobs:latest",
        command="python3 -m jobs.silver_energy",
        network_mode="energy-pipeline_pipeline_net",
        environment={
            "AZURE_STORAGE_ACCOUNT_NAME": "{{ var.value.azure_storage_account_name }}",
            "AZURE_STORAGE_ACCOUNT_KEY": "{{ var.value.azure_storage_account_key }}",
        },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )
    run_dbt = DockerOperator(
    task_id="run_dbt",
    image="energy-pipeline-dbt:latest",
    command="bash -c 'cd /app/energy_pipeline && dbt run --profiles-dir ..'",
    network_mode="energy-pipeline_pipeline_net",
    environment={
        "AZURE_STORAGE_ACCOUNT_NAME": "{{ var.value.azure_storage_account_name }}",
        "AZURE_STORAGE_ACCOUNT_KEY": "{{ var.value.azure_storage_account_key }}",
    },
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    )

    run_dbt_test = DockerOperator(
    task_id="run_dbt_test",
    image="energy-pipeline-dbt:latest",
    command="bash -c 'cd /app/energy_pipeline && dbt test --profiles-dir ..'",
    network_mode="energy-pipeline_pipeline_net",
    environment={
        "AZURE_STORAGE_ACCOUNT_NAME": "{{ var.value.azure_storage_account_name }}",
        "AZURE_STORAGE_ACCOUNT_KEY": "{{ var.value.azure_storage_account_key }}",
    },
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    )

    [run_weather_producer, run_energy_producer]
    run_weather_producer >> wait_for_weather_bronze
    run_energy_producer  >> wait_for_energy_bronze
    [wait_for_weather_bronze, wait_for_energy_bronze] >> run_silver_weather
    [wait_for_weather_bronze, wait_for_energy_bronze] >> run_silver_energy
    [run_silver_weather, run_silver_energy] >> run_dbt
    run_dbt >> run_dbt_test