# src/Producer_api/tests/test_producer_integration.py
import pytest
from test_producer_direct import run_test as run_direct_test
from test_producer_versions import VersionTester

@pytest.mark.integration
def test_direct_producer():
    """Интеграционный тест для test_producer_direct.py"""
    run_direct_test(
        num_messages=10,
        versions=["number_id_v1", "string_id_v1"],
        formats=["json", "protobuf"],
        max_workers=5
    )

@pytest.mark.integration
def test_version_tester():
    """Интеграционный тест для test_producer_versions.py"""
    tester = VersionTester()
    tester.run_test(
        num_messages=10,
        versions=["number_id_v1", "string_id_v1"],
        formats=["json", "protobuf"],
        max_workers=5
    )