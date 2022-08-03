
from region_runner.get_esi.get_markets import get_structure_data, get_region_data



def test_get_structure_data():
    resp = get_structure_data(1039149782071)
    assert resp

def test_get_region_data():
    resp = get_region_data(10000002)
    assert resp

