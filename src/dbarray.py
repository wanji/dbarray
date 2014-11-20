#!/usr/bin/env python
# coding: utf-8

#########################################################################
#########################################################################

"""
   File Name: dbarray.py
      Author: Wan Ji
      E-mail: wanji@live.com
  Created on: Sun Mar 23 15:23:27 2014 CST
"""
DESCRIPTION = """
Array stored in DB.
"""

import os
import struct
import logging

import numpy as np

import storage

DBTYPE = {
    "leveldb":  storage.StorageLevelDB,
    "lmdb":     storage.StorageLMDB,
    # "redis":    storage.StorageRedis,
}

# storing the number in `long long` type
PACK_NUM_TYPE_u8 = 'B'
PACK_NUM_TYPE_i8 = 'b'
PACK_NUM_TYPE_u16 = 'H'
PACK_NUM_TYPE_i16 = 'h'
PACK_NUM_TYPE_u32 = 'I'
PACK_NUM_TYPE_i32 = 'i'
PACK_NUM_TYPE_u64 = 'Q'
PACK_NUM_TYPE_i64 = 'q'
PACK_NUM_TYPE = PACK_NUM_TYPE_i64

TSTR_NDARRAY = 'nda'
TSTR_INT = 'int'
TSTR_STR = 'str'


class DBArray(object):
    """ Array stored in database.

    `DBArray` is an 2D array stored in database.
    The purpose of this class is to provide a way to store and access large
    array which can not be loaded into memory.

    An associated data-type object describes the format of each element in the
    array (its byte-order, how many bytes it occupies in memory, whether it is
    an integer, a floating point number, or something else, etc.)
    """

    def __init__(self, dbpath, dbtype='leveldb'):
        """ Init.
        """
        ## Number of rows in the array
        self.nrows = -1
        ## Number of cols in the array
        self.ncols = -1
        ## Associated data-type describes the format of each element in the
        # array, which is compact with `numpy`
        self.dtype = None

        is_exists = os.path.exists(dbpath)

        self._storage = DBTYPE[dbtype](dbpath)

        # load information from existing DB
        if is_exists:
            self._loadinfo()
        else:
            self.set_shape((self.nrows, self.ncols))
            self.set_dtype(self.dtype)

    def set_shape(self, shape):
        """ Set shape of DBArray
        """
        (self.nrows, self.ncols) = shape
        self._storage.set('nrows', struct.pack(PACK_NUM_TYPE, shape[0]))
        self._storage.set('ncols', struct.pack(PACK_NUM_TYPE, shape[1]))

    @classmethod
    def _get_dtype_name(cls, dtype):
        """ Get the name of data type
        """
        if dtype is None:
            return 'None'
        elif type(dtype) is type:
            return dtype.__name__
        elif type(dtype) is np.dtype:
            return str(dtype)
        elif type(dtype) is str:
            return str
        else:
            raise Exception('Unrecognized data type: %s' % str(dtype))

    @classmethod
    def _gen_dtype(cls, dtype_str):
        """ Generate dtype from name
        """
        if dtype_str == 'None':
            return None
        else:
            return eval('np.' + dtype_str)

    def set_dtype(self, dtype):
        """ Set shape of DBArray
        """
        dtype_str = self._get_dtype_name(dtype)
        self.dtype = self._gen_dtype(dtype_str)
        self._storage.set('dtype', dtype_str)

    def _loadinfo(self):
        """ Load information from DB
        """
        self.nrows = struct.unpack(PACK_NUM_TYPE, self._storage.get('nrows'))[0]
        self.ncols = struct.unpack(PACK_NUM_TYPE, self._storage.get('ncols'))[0]
        self.dtype = self._gen_dtype(self._storage.get('dtype'))

    @classmethod
    def _parse_key(cls, key, stop=0):
        """ Parse the provided key.

        Parameters
        ----------
            key: integer or slice
                Valide key can be of type `int`, `long` or `slice`.
            stop: integer
                Upper bound of the keys.

        Returns
        -------
            keylist: list or None
                list of valid keys induced from key.
        """
        if type(key) in [int, long]:
            return [key]
        elif type(key) is slice:
            return range(0 if key.start is None else key.start,
                         stop if key.stop is None else key.stop,
                         1 if key.step is None else key.step)
        elif type(key) is list:
            return key
        else:
            return None

    def __len__(self):
        """ len()
        """
        return self.nrows

    def __getitem__(self, key):
        """ Get values from DB.

        Parameters
        ----------
            key: integer, slice or tuple
                Valide key can be of type `int`, `long` or `slice`
                or tuple of the previous types.

        Returns
        -------
            subarray: numpy.ndarray
                rows and cols specified by key
        """
        v_rid = None
        v_cid = None
        if type(key) is not tuple:
            # get rows
            v_rid = self._parse_key(key, self.nrows)
            if None == v_rid:
                logging.error("Error: key must be a tuple or integer!")
                return None
        else:
            if len(key) == 0:
                # bad key
                logging.error("Error: invalid syntax!")
                return None
            elif len(key) > 2:
                # bad key
                logging.error("Error: too many indices!")
                return None
            elif len(key) == 1:
                # get rows
                v_rid = self._parse_key(key[0], self.nrows)
                v_cid = None
            elif len(key) == 2:
                # get rows and then select cols
                v_rid = self._parse_key(key[0], self.nrows)
                v_cid = self._parse_key(key[1], self.ncols)

        rows = self.get_rows(v_rid)
        if None != v_cid:
            rows = rows[:, v_cid]
        return rows

    def get_rows(self, v_rid):
        """ Get rows from DB
        """
        nrows = len(v_rid)
        resarr = np.ndarray((nrows, self.ncols), self.dtype)
        for i in range(nrows):
            resarr[i, :] = self.get_row(v_rid[i])
        return resarr

    def set_rows(self, v_rid, arr):
        """ Set rows of DB
        """
        nrows = len(v_rid)
        for i in range(nrows):
            self.set_row(v_rid[i], arr[i, :])

    def get_row(self, rid):
        """ Get a row
        """
        return np.ndarray(self.ncols, self.dtype,
                          self._storage.get(struct.pack(PACK_NUM_TYPE, rid)))

    def set_row(self, rid, arr):
        """ Set a row
        """
        return self._storage.set(struct.pack(PACK_NUM_TYPE, rid), arr.data)

    def set_db_attr(self, key, val):
        """
        Set attribute.

        The following attribute type are supported:
            'nda': numpy.ndarray
            'int': int
            'str': string
        """
        if type(val) is np.ndarray:
            dtype_str = self._get_dtype_name(val.dtype)
            self.set_db_attr(key + "_dtype", dtype_str)
            return self._storage.set(key, TSTR_NDARRAY +
                                    val.tostring())
        elif type(val) is int:
            return self._storage.set(key, TSTR_INT +
                                    struct.pack(PACK_NUM_TYPE, val))
        elif type(val) is str:
            return self._storage.set(key, TSTR_STR + val)
        else:
            raise('Unsupported attribute type: %s' % str(type(val)))

    def get_db_attr(self, key):
        """
        Set attribute
        """
        rawval = self._storage.get(key)
        # ndarray: `attr_dtype` is stored in `$key'_dtype'`
        if rawval[:len(TSTR_NDARRAY)] == TSTR_NDARRAY:
            attr_dtype = self._gen_dtype(
                self.get_db_attr(key + "_dtype"))
            return np.fromstring(rawval[len(TSTR_NDARRAY):],
                                 dtype=attr_dtype)
        # int
        elif rawval[:len(TSTR_INT)] == TSTR_INT:
            return struct.unpack(PACK_NUM_TYPE,
                                 rawval[len(TSTR_INT):])[0]
        # string
        elif rawval[:len(TSTR_STR)] == TSTR_STR:
            return rawval[len(TSTR_STR):]
        # unknown type
        else:
            raise('Unknown attribute type: %s' % rawval[:8])

    def __del__(self):
        """ Destroy
        """
        pass

    @classmethod
    def fromndarray(cls, arr, dbpath, dbtype='leveldb'):
        """ Construct DBArray from `ndarray`
        """
        dba = DBArray(dbpath, dbtype)
        dba.set_dtype(arr.dtype)
        dba.set_shape(arr.shape)
        for rid in range(arr.shape[0]):
            dba.set_row(rid, arr[rid, :])
        return dba

    def tondarray(self):
        """ Convert to ndarray
        """
        return self.get_rows(range(self.nrows))
