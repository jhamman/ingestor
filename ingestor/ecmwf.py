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
        '''
        Create a new ECMWFDataSet

        This class manages a series of related requests by submitting and
        downloading datasets from ECMWF in parallel.

        Parameters
        ----------
        variables : list-like
            List of variables to download.
        data_dir : str
            Path to put downloaded files.
        file_template : str
            Filename template for downloaded files
        options : dict, optional
            Dictionary of additional options to provide to
            `ECMWFDataServer.retrieve`.

        See also
        --------
        ECMWFDataServer.retrieve
        '''
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
        '''
        Return a new DataSet by selecting index bounds along the specified
        temporal and/or spatial dimension(s)

        Parameters
        ----------
        time : slice, optional
            Time period to select (e.g. `slice('1979-09-01', '2015-12-31')`).
        lat : slice, optional
            Latitude bounds to select (e.g. `slice(20., 50.)`).
        lon : slice, optional
            longitude bounds to select (e.g. `slice(-120.0, -90.)`).
        '''

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
        '''call the server retrieve method, the sleep allows for metering'''
        if sleep is not None:
            time.sleep(sleep)
        self.server.retrieve(r)

    def load(self, n_jobs=20, **kwargs):
        '''run the download jobs in parallel, return a single xarray dataset

        Parameters
        ----------
        n_jobs : int, optional
            Number of parallel requests / downloads to perform.
        kwargs :
            Additional keyword arguments to pass to joblib.Parallel

        See Also
        --------
        joblib.parallel
        '''

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
