import subprocess
import sys


def test_data_quality_pipeline():
    result = subprocess.run(
        [sys.executable, "-m", "src.run_dq_tests"],
        check=False,
    )

    assert result.returncode == 0, (
        "Data quality tests failed. "
        "See the DQ test output above for details."
    )
