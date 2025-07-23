# src/Consumer_api/app/tests/test_load.py
# src/Consumer_api/app/tests/test_load.py
import pytest
import requests
import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.mark.integration
class TestLoadPerformance:
    BASE_URL = "http://localhost:8002/api"
    TEST_DURATION = timedelta(seconds=15)
    WORKERS = 3
    REQUEST_TIMEOUT = 3

    @pytest.mark.parametrize("version", [1, 2])
    def test_version_under_load(self, version):
        """Нагрузочное тестирование версий API"""
        stats = {
            'total': 0,
            'success': 0,
            'errors': 0,
            'response_times': []
        }

        def make_request():
            try:
                start = time.time()
                response = requests.get(
                    f"{self.BASE_URL}/v{version}/messages",
                    params={"limit": 5},
                    timeout=self.REQUEST_TIMEOUT
                )
                elapsed = time.time() - start
                return response.status_code == 200, elapsed
            except Exception as e:
                logger.warning(f"Request failed: {str(e)}")
                return False, 0

        start_time = datetime.now()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.WORKERS) as executor:
            futures = []
            
            while (datetime.now() - start_time) < self.TEST_DURATION:
                futures.append(executor.submit(make_request))
                time.sleep(0.1)

            for future in concurrent.futures.as_completed(futures):
                try:
                    success, elapsed = future.result()
                    stats['total'] += 1
                    if success:
                        stats['success'] += 1
                        stats['response_times'].append(elapsed)
                    else:
                        stats['errors'] += 1
                except Exception as e:
                    logger.error(f"Error processing future: {str(e)}")
                    stats['errors'] += 1

        if stats['total'] == 0:
            pytest.skip("No requests were made - service unavailable")

        success_rate = stats['success'] / stats['total']
        avg_response = sum(stats['response_times'])/len(stats['response_times']) if stats['response_times'] else 0

        logger.info(f"\nLoad test v{version} results:")
        logger.info(f"Requests: {stats['total']}")
        logger.info(f"Success rate: {success_rate:.1%}")
        logger.info(f"Avg response: {avg_response:.3f}s")

        assert success_rate > 0.7, f"Success rate below 70% ({success_rate:.1%})"
        assert avg_response < 1.0, f"Average response time too high ({avg_response:.3f}s)"