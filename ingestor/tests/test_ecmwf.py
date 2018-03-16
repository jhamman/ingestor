
from ingestor.ecmwf import ECMWFDataSet


def test_init_dataset():

    variables = ['Temperature', 'Pressure']
    ds = ECMWFDataSet(variables)
    assert ds.variables == variables
    assert isinstance(ds.request, dict)

    ds = ECMWFDataSet(variables, data_dir='/foo/bar',
                      file_template='data_%Y-%m.nc')
    assert ds._file_template == '/foo/bar/data_%Y-%m.nc'


def test_repr():
    variables = ['Temperature', 'Pressure']
    ds = ECMWFDataSet(variables)

    actual = repr(ds)

    for expected in ['Selection Criteria:', 'Data variables:',
                     'Request Parameters'] + variables:
        assert expected in actual

    ds = ds.sel(time=slice('1990', '2000'))
    actual = repr(ds)

    assert '1990' in actual


def test_data_vars():
    variables = ('Temperature', 'Pressure')
    ds = ECMWFDataSet(variables)
    assert ds.data_vars == list(variables)
    assert isinstance(ds.data_vars, list)


def test_sel_time():
    variables = ('Temperature', 'Pressure')
    ds = ECMWFDataSet(variables)

    ds = ds.sel(time=slice('1990-01', '1990-09-15'))

    assert ds.start == '1990-01'
    assert ds.stop == '1990-09-15'

    assert len(ds.filenames) == 9
    assert len(ds.dates) == 9
    assert '1990-01-01' in ds.dates[0]
    assert '1990-01-31' in ds.dates[0]
    assert '1990-09-15' in ds.dates[-1]


def test_sel_area():
    variables = ('Temperature', 'Pressure')
    ds = ECMWFDataSet(variables)

    ds = ds.sel(lat=slice(-20, 20), lon=slice(-120, -85))

    assert '-20' in ds.request['area']
    assert '-120' in ds.request['area']
    assert '-85' in ds.request['area']


def test_requests():
    variables = ('Temperature', 'Pressure')
    ds = ECMWFDataSet(variables)
    ds = ds.sel(time=slice('1990-01', '1990-09-15'),
                lat=slice(-20, 20), lon=slice(-120, -85))

    requests = ds.requests
    assert len(requests) == 9
    assert isinstance(requests, list)
    assert all([isinstance(r, dict) for r in requests])
    assert all(['area' in r for r in requests])


def test_load():
    '''test that this works, will need to think about how to do this with ci'''
    pass  # TODO


def test_cleanup():
    '''mock the load call'''
    pass  # TODO
