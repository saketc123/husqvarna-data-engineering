import subprocess
import sys


STAGES = [
    ("Bronze ingestion", "src.build_bronze"),
    ("Silver transformation", "src.build_silver"),
    ("Gold transformation", "src.build_gold"),
    ("Data quality tests", "src.run_dq_tests"),
]


def run_stage(stage_name, module_name):
    print("=" * 70)
    print(f"STARTING: {stage_name}")
    print("=" * 70)

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"{stage_name} failed with exit code "
            f"{result.returncode}"
        )

    print("=" * 70)
    print(f"COMPLETED: {stage_name}")
    print("=" * 70)


def main():
    print("=" * 70)
    print("HUSQVARNA DATA ENGINEERING PIPELINE")
    print("=" * 70)

    for stage_name, module_name in STAGES:
        run_stage(stage_name, module_name)

    print("=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    main()