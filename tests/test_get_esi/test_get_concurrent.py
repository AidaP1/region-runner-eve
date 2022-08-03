
from region_runner.get_esi.get_concurrent import get_concurrent_reqs, get_first_page



def test_get_concurrent_reqs():
    result = get_concurrent_reqs()

    assert result

def test_get_first_page():
    result = get_first_page()

    assert result