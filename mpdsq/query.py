import json
import os
import re
import requests

__all__ = ['MPDSQueryEngine']


class _Structures(object):
    def __init__(self, query_engine):
        self._qe = query_engine

    def find(self, query, fmt=None):
        if fmt is None:
            fmt = self._qe.fmt

        for result in self._qe.find(query, fmt=fmt):
            if fmt == 'json':
                try:
                    if result['object_type'] == 'S':
                        yield result
                except KeyError:
                    continue
            elif fmt == 'cif':
                if not result.startswith('data_error'):
                    yield result


class _Properties(object):
    def __init__(self, query_engine):
        self._qe = query_engine

    def find(self, query):
        for result in self._qe.find(query, fmt='json'):
            try:
                if result['object_type'] == 'P':
                    yield result
            except KeyError:
                continue


class _Iterator(object):
    def __init__(self, query_engine, query, fmt):
        self._query_engine = query_engine
        self._query_str = json.dumps(query)
        self._fmt = fmt
        self._page = 0
        self._pagesize = query_engine.pagesize

        self._entry_idx = 0
        self._page_entries = None

    def __iter__(self):
        return self

    def next(self):
        for page in range(0, npages):
            r = self.get(q=json.dumps(query), fmt=fmt, pagesize=pagesize, page=page)

            if fmt == 'json':
                result = json.loads(r.text)
                if (page + 1) * pagesize > count:
                    last = count - (page * pagesize)
                else:
                    last = pagesize
                for i in range(0, last):
                    yield result['out'][i]
            elif fmt == 'cif':
                lines = r.text.splitlines()
                cif = []
                for line in lines:
                    if cif:
                        if line.startswith('data_'):
                            text = "\n".join(cif)
                            cif = [line]
                            yield text
                        else:
                            cif.append(line)
                    else:
                        if line.startswith('data_'):
                            cif.append(line)
                if cif:
                    yield "\n".join(cif)

    class CifAggregate(object):
        def __init__(self, header):
            self._cif = [header]

        def append(self, line):
            self._cif.append(line)

        def get(self):
            return "\n".join(self._cif)

    def _get_page(self):
        r = self._query_engine.get(
            q=self._query_str,
            fmt=self._fmt,
            pagesize=self._pagesize,
            page=self._page
        )
        if self._fmt == 'json':
            result = json.loads(r.text)
            self._page_entries = result['out']
        elif self._fmt == 'cif':
            self._page_entries = []
            lines = r.text.splitlines()
            cif = None
            for line in lines:
                if line.startswith('data_'):
                    # Start of new entry
                    if cif:  # Store current
                        self._page_entries.append(cif.get())

                    cif = self.CifAggregate(line)
                elif cif:
                    cif.append(line)
            if cif:
                self._page_entries.append(cif.get())


class MPDSQueryEngine(object):
    # class defaults
    pagesize = 1000
    fmt = 'json'
    url = "https://api.mpds.io/v0/download/facet"

    def __init__(self, url=None, apikey=None):
        if url is not None:
            self.url = url
        if apikey is None:
            try:
                self._apikey = os.environ['MPDS_KEY']
            except KeyError:
                raise ValueError('API key not supplied and MPDS_KEY env variable not set')
        else:
            self._apikey = apikey
        self._structures = _Structures(self)
        self._properties = _Properties(self)

    @property
    def properties(self):
        """Access the properties collection in the MPDS"""
        return self._properties

    @property
    def structures(self):
        """Access the structures collection in the MPDS"""
        return self._structures

    def get(self, **kwargs):
        return requests.get(
            url=self.url, params=kwargs, headers={'Key': self._apikey})

    def get_counts(self, query, pagesize=None):
        if not isinstance(query, dict):
            raise TypeError("Query should be a dictionary")

        if pagesize is None:
            pagesize = self.pagesize

        r = self.get(q=json.dumps(query), fmt='json', pagesize=pagesize)
        if not r.ok:
            raise RuntimeError("Request failed: {}".format(r.text))

        res = json.loads(r.text)
        if not r.ok:
            raise ValueError("Got error response: '{}'".format(res['error']))

        return res['count'], res['npages']

    def find(self, query, fmt=None):
        if not isinstance(query, dict):
            raise TypeError("Query should be a dictionary")

        pagesize = 1000
        if fmt is None:
            fmt = self.fmt

        count, npages = self.get_counts(query)

        for page in range(0, npages):
            r = self.get(q=json.dumps(query), fmt=fmt, pagesize=pagesize, page=page)

            if fmt == 'json':
                result = json.loads(r.text)
                if (page + 1) * pagesize > count:
                    last = count - (page * pagesize)
                else:
                    last = pagesize
                for i in range(0, last):
                    yield result['out'][i]
            elif fmt == 'cif':
                lines = r.text.splitlines()
                cif = []
                for line in lines:
                    if cif:
                        if line.startswith('data_'):
                            text = "\n".join(cif)
                            cif = [line]
                            yield text
                        else:
                            cif.append(line)
                    else:
                        if line.startswith('data_'):
                            cif.append(line)
                if cif:
                    yield "\n".join(cif)

    def get_all(self, query, fmt):
        r = self.get(q=query, fmt='json', pagezize=1000)
        res = json.loads(r.text)
        npages = res['npages']

        if fmt == 'json':
            for p in range(1, npages):
                r = self.get(q=query, fmt=json, pagezize=1000, page=p)
                res['out'].extend(json.loads(r.text)['out'])
            return res
        elif fmt == 'cif':
            responses = []
            for p in range(0, npages):
                r = self.get(q=query, fmt='cif', pagezize=1000, page=p)
                responses.append(r.text)
            return "".join(responses)
        else:
            raise ValueError("Unknown format '{}'".format(fmt))
