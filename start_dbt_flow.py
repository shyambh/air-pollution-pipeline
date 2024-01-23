import os
from pathlib import Path
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow


@flow
def trigger_dbt_flow(is_test_env: bool) -> str:
    result = DbtCoreOperation(
        commands=[
            f"dbt build --full-refresh --vars '{{\"is_test_run\": {is_test_env}}}'"
        ],
        project_dir=Path() / "custom_models",
        profiles_dir=Path().home() / ".dbt",
        stream_output=True,
    ).run()
    return result


# if __name__ == "__main__":
#     trigger_dbt_flow()


def run_dbt_flow(is_test_env):
    trigger_dbt_flow(is_test_env)
