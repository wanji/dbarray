#!/usr/bin/env python
# coding: utf-8

#########################################################################
#########################################################################

"""
   File Name: test_dbarray.py
      Author: Wan Ji
      E-mail: wanji@live.com
  Created on: Thu Nov 20 09:05:56 2014 CST
"""
DESCRIPTION = """
Test the DBArray with LMDB backend.
"""

import unittest

import os
import tempfile

import numpy as np
import numpy.random as nr
from dbarray import DBArray


class CommTestDBArray(object):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        shape = (100, 256)
        cls.commdbs = {
            'float32': np.require(nr.random(shape), np.float32),
            'float64': np.require(nr.random(shape), np.float64),
            'int64':   np.require(nr.random(shape) * 100, np.int64),
            'int32':   np.require(nr.random(shape) * 100, np.int32),
        }

    @classmethod
    def tearDownClass(cls):
        os.system('rm -r %s' % cls.tempdir)

    def _arr_eq(self, arr1, arr2):
        """ Check if two `ndarray`s are equal.
        """
        self.assertEqual(np.abs(arr1 - arr2).sum(), 0)

    def _check_info(self, dba, nrows, ncols, dtype):
        """ Check if the basic information of `dba` is correct.
        """
        self.assertEqual(nrows, dba.nrows)
        self.assertEqual(ncols, dba.ncols)
        self.assertEqual(dtype, dba.dtype.name)

    def _info_eq(self, dba, arr):
        """ Check if the basic information of `dba` is the same as `arr`.
        """
        self.assertEqual(arr.shape[0], dba.nrows)
        self.assertEqual(arr.shape[1], dba.ncols)
        self.assertEqual(arr.dtype, dba.dtype)

    def _create_dba(self, dbpath, shape, dtype):
        """ Initialize a `DBArray` with provided information.
        """
        dba = DBArray(dbpath, self.DBTYPE)
        dba.set_shape(shape)
        dba.set_dtype(dtype)
        return dba

    def test_init(self):
        dbpath = os.path.join(self.tempdir, self.DBTYPE, 'test_init.db')
        nrows = 100
        ncols = 1024

        # test different kinds of dtype
        for dtype in ['float32', np.float32, np.dtype('float32')]:
            # create new DBArray
            dba = self._create_dba(dbpath, (nrows, ncols), np.dtype(dtype))
            self._check_info(dba, nrows, ncols,
                             dtype if type(dtype) is not type
                             else dtype.__name__)

            # close and re-open
            del dba
            dba = DBArray(dbpath, self.DBTYPE)
            self._check_info(dba, nrows, ncols,
                             dtype if type(dtype) is not type
                             else dtype.__name__)
            del dba

    def test_from_and_to(self):
        for key, val in self.commdbs.iteritems():
            dbpath = os.path.join(self.tempdir, self.DBTYPE,
                                  'test_from_%s.db' % key)
            dba = DBArray.fromndarray(val, dbpath, self.DBTYPE)
            self._info_eq(dba, val)

            arr = dba.tondarray()
            self._arr_eq(arr, val)

            # cloase and re-open
            del dba
            dba = DBArray(dbpath, self.DBTYPE)
            self._info_eq(dba, val)

            arr = dba.tondarray()
            self._arr_eq(arr, val)

    def test_get_data(self):
        for key, val in self.commdbs.iteritems():
            dbpath = os.path.join(self.tempdir, self.DBTYPE,
                                  'test_get_data_%s.db' % key)
            dba = DBArray.fromndarray(val, dbpath, self.DBTYPE)
            self._arr_eq(dba[10], val[10])
            self._arr_eq(dba[10, :], val[10, :])
            self._arr_eq(dba[1:10], val[1:10])
            self._arr_eq(dba[1:10, :], val[1:10, :])

            self._arr_eq(dba[[1, 2, 5]], val[[1, 2, 5]])

    def test_set_data(self):
        for key, val in self.commdbs.iteritems():
            dbpath = os.path.join(self.tempdir, self.DBTYPE,
                                  'test_set_data_%s.db' % key)
            dba = self._create_dba(dbpath, val.shape, val.dtype)

            # set rows of
            start_id = 0
            num_rows = 1
            while start_id < val.shape[0]:
                num_rows = min(num_rows, val.shape[0]-start_id)
                end_id = start_id + num_rows
                dba[start_id:end_id] = val[start_id:end_id]
                start_id = end_id
                num_rows += 1

            self._info_eq(dba, val)
            arr = dba.tondarray()
            self._arr_eq(arr, val)

    def test_attr(self):
        """ Set/Get attributes.
        """
        for key, val in self.commdbs.iteritems():
            dbpath = os.path.join(self.tempdir, self.DBTYPE,
                                  'test_attr_%s.db' % key)
            dba = DBArray.fromndarray(val, dbpath, self.DBTYPE)
            data_mean = val.mean(0)
            int_attr = np.random.randint(100)
            flt_attr = np.random.rand()
            str_attr = 'hello world'

            dba['data_mean'] = data_mean
            dba['int_attr'] = int_attr
            dba['str_attr'] = str_attr
            # float is not support currently
            self.assertRaises(TypeError, dba.__setitem__,
                              ('flt_attr', flt_attr))

            self._arr_eq(dba['data_mean'], data_mean)
            self.assertEqual(dba['int_attr'], int_attr)
            self.assertEqual(dba['str_attr'], str_attr)

            del dba
            dba = DBArray(dbpath, self.DBTYPE)

            self._arr_eq(dba['data_mean'], data_mean)
            self.assertEqual(dba['int_attr'], int_attr)
            self.assertEqual(dba['str_attr'], str_attr)

    def test_set_attr(self):
        for key, val in self.commdbs.iteritems():
            dbpath = os.path.join(self.tempdir, self.DBTYPE,
                                  'test_set_attr_%s.db' % key)
            dba = self._create_dba(dbpath, val.shape, val.dtype)

            # set rows of
            start_id = 0
            num_rows = 1
            while start_id < val.shape[0]:
                num_rows = min(num_rows, val.shape[0]-start_id)
                end_id = start_id + num_rows
                dba[start_id:end_id] = val[start_id:end_id]
                start_id = end_id
                num_rows += 1

            self._info_eq(dba, val)
            arr = dba.tondarray()
            self._arr_eq(arr, val)


class TestDBArray_LevelDB(unittest.TestCase, CommTestDBArray):
    DBTYPE = 'leveldb'

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        shape = (100, 256)
        cls.commdbs = {
            'float32': np.require(nr.random(shape), np.float32),
            'float64': np.require(nr.random(shape), np.float64),
            'int64':   np.require(nr.random(shape) * 100, np.int64),
            'int32':   np.require(nr.random(shape) * 100, np.int32),
        }
        os.system('mkdir %s' % os.path.join(cls.tempdir, cls.DBTYPE))

    @classmethod
    def tearDownClass(cls):
        os.system('rm -r %s' % cls.tempdir)


class TestDBArray_LMDB(unittest.TestCase, CommTestDBArray):
    DBTYPE = 'lmdb'

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        shape = (100, 256)
        cls.commdbs = {
            'float32': np.require(nr.random(shape), np.float32),
            'float64': np.require(nr.random(shape), np.float64),
            'int64':   np.require(nr.random(shape) * 100, np.int64),
            'int32':   np.require(nr.random(shape) * 100, np.int32),
        }
        os.system('mkdir %s' % os.path.join(cls.tempdir, cls.DBTYPE))

    @classmethod
    def tearDownClass(cls):
        os.system('rm -r %s' % cls.tempdir)


if __name__ == '__main__':
    unittest.main()
