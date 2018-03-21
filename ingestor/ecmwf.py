import os
import time

import pandas as pd
import xarray as xr

from ecmwfapi import ECMWFDataServer
from joblib import Parallel, delayed


class ECMWFDataSet(object):
    '''kind of like an xarray.Dataset but lazily evaluated from ECMWF'''

    def __init__(self, variables, data_dir='./',
                 file_template='ecmwf_data_%Y-%m.nc', options=None):
        self.server = ECMWFDataServer()

        self.variables = list(variables)

        self.request = dict(param='/'.join(variables), format='netcdf')
        if options is not None:
            self.request.update(options)

        self._file_template = os.path.join(data_dir, file_template)
        self.filenames = []

        self.start = None
        self.stop = None
        self.dates = []

    def __repr__(self):
        summary = [u'<ingestor.%s>' % type(self).__name__]
        summary.append(u'Selection Criteria:')
        summary.append(u'    Time: %s-%s' % (self.start, self.stop))
        summary.append(u'    Area: %s' % (self.request.get('area', 'None')))
        summary.append(u'Data variables:')
        for var in self.variables:
            summary.append(u'    %s' % var)
        summary.append(u'Request Parameters:')
        for k, v in self.request.items():
            summary.append(u'    %s: %s' % (k, v))
        return u'\n'.join(summary)

    @property
    def data_vars(self):
        return list(self.variables)

    def sel(self, time=None, lat=None, lon=None):

        if lat or lon:
            # area:  N/W/S/E
            self.request['area'] = '%f/%f/%f/%f' % (lat.stop, lon.start,
                                                    lat.start, lon.stop)

        if time:
            self.start = time.start
            self.stop = time.stop
            # start of month
            starts = pd.date_range(start=time.start, end=time.stop, freq='MS')
            # end of month
            ends = starts + pd.offsets.MonthEnd()
            # max sure we honor the stop
            ends.values[-1] = pd.Timestamp(time.stop)

            dt_format = '%Y-%m-%d'
            self.filenames = starts.strftime(self._file_template)
            self.dates = ["{}/to/{}".format(start, end)
                          for start, end in zip(starts.strftime(dt_format),
                                                ends.strftime(dt_format))]

        return self

    @property
    def requests(self):
        '''make the requests dictionaries on the fly'''
        if not self.dates:
            return [dict(target=target) for target in self.filenames]
        return [dict(date=date, target=target, **self.request)
                for date, target in zip(self.dates, self.filenames)]

    def _retrieve(self, r, sleep=None):
        if sleep is not None:
            time.sleep(sleep)
        self.server.retrieve(r)

    def load(self, n_jobs=20, **kwargs):
        '''run the download jobs in parallel, return a single xarray dataset'''

        # sleep a short time so that we don't get the error:
        # HTTP Error 429: Too Many Requests
        Parallel(n_jobs=n_jobs, **kwargs)(delayed(self._retrieve)(
            r, sleep=i*1) for i, r in enumerate(self.requests))

        # reconsitute a xarray dataset
        ds = xr.open_mfdataset(self.filenames)

        return ds

    def cleanup(self):
        '''remove temporary files'''
        for f in self.filenames:
            os.remove(f)
