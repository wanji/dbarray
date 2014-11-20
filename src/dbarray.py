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
from struct import pack, unpack
import logging

import numpy as np

import storage

DBTYPE = {
    "leveldb":  storage.StorageLevelDB,
    "lmdb":     storage.StorageLMDB,
    # "redis":    storage.StorageRedis,
}
DEFAULT_DTYPE = 'lmdb'

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
    array. (The data-type is compact with `numpy`)
    """

    def __init__(self, dbpath, dbtype=DEFAULT_DTYPE):
        """ Initialize the `DBArray`

        Args:
            `dbpath`    [str]   path of the database.
            `dbtype`    [str]   type of the database.

        Returns: N/A
        """

        ## Number of rows in the array
        self.nrows = -1
        ## Number of cols in the array
        self.ncols = -1
        ## Associated data-type describes the format of each element in the
        # array, which is compact with `numpy`
        self.dtype = None

        is_exists = os.path.exists(dbpath)

        C_Storage = DBTYPE[dbtype]
        # `dbpath` exists but is not `dbtype`
        if is_exists and not C_Storage.is_valid(dbpath):
            logging.warning('`%s` exists but is not `%s`' % (dbpath, dbtype))
            hit_cnt = 0
            for othertype in DBTYPE.keys():
                if othertype == dbtype:
                    continue
                logging.warning('TRY type: %s' % othertype)
                C_Storage = DBTYPE[othertype]
                if C_Storage.is_valid(dbpath):
                    logging.warning('HIT! %s' % othertype)
                    hit_cnt += 1
            if hit_cnt == 0:
                logging.fatal(
                    '`%s` exists but the DB type is unknown!' % dbpath)
            elif hit_cnt > 1:
                logging.fatal(
                    '`%s` exists but matches too many DB types: %d!' %
                    dbpath, hit_cnt)
            else:
                logging.warning(
                    'Using `%s` instead of `%s`' % (othertype, dbtype))

        self._storage = C_Storage(dbpath)

        # load information from existing DB
        if is_exists:
            self._loadinfo()
        else:
            self.set_shape((self.nrows, self.ncols))
            self.set_dtype(self.dtype)

    def __del__(self):
        """ Destroy the `DBArray`

        Args: N/A

        Returns: N/A
        """
        pass

    def __len__(self):
        """ Get number of rows.
        """
        return self.nrows

    def __getitem__(self, key):
        """ Get a subarray from DB.

        Args:
            `key`   [str, integer, slice or tuple]
                An `key` of type `str` corresponds to an attribute.
                Other kinds of `key`s correspond to array indices.

        Returns:
            `attr`      [str, int or numpy.ndarray] for `key` of type `str`.
            `subarray`  [numpy.ndarray] for `key` of other types.
        """
        if type(key) is str:
            return self.get_db_attr(key)
        else:
            v_rid, v_cid = self._parse_key_for_array(
                key, self.nrows, self.ncols)

            if None == v_rid:
                logging.error("Invalid key: %s" % str(key))
                return None

            rows = self.get_rows(v_rid)
            if None != v_cid:
                rows = rows[:, v_cid]
            return rows

    def __setitem__(self, key, val):
        """ Set a subarray in DB.

        Args:
            `key`   [str, integer, slice or tuple]
                An `key` of type `str` corresponds to an attribute.
                Other kinds of `key`s correspond to array indices.
            `val`
                [str, int or numpy.ndarray] for `key` of type `str`.
                [numpy.ndarray] for `key` of other types.

        Returns: N/A
        """
        if type(key) is str:
            return self.set_db_attr(key, val)
        else:
            if type(val) is not np.ndarray:
                logging.error("Invalid value: %s" % str(val))
                return

            v_rid, v_cid = self._parse_key_for_array(
                key, self.nrows, self.ncols)

            if None == v_rid:
                logging.error("Invalid key: %s" % str(key))
                return

            val = val.reshape(len(v_rid), -1)

            if v_cid is None:
                rows = val
            else:
                rows = self.get_rows(v_rid)
                rows[:, v_cid] = val
            self.set_rows(v_rid, rows)

    def set_shape(self, shape):
        """ Set shape of `DBArray`.

        Args:
            `shape` [tuple of int: (nrows, ncols)]
                Specify number of rows and cols of `DBArray`.

        Returns: N/A
        """
        (self.nrows, self.ncols) = shape
        self._storage.set('nrows', pack(PACK_NUM_TYPE, shape[0]))
        self._storage.set('ncols', pack(PACK_NUM_TYPE, shape[1]))

    def set_dtype(self, dtype):
        """ Set dtype of `DBArray`.

        Args:
            `dtype` [str or numpy.dtype]
                Can be any valid numpy.dtype.

        Returns: N/A
        """
        dtype_str = self._get_dtype_name(dtype)
        self.dtype = self._gen_dtype(dtype_str)
        self._storage.set('dtype', dtype_str)

    def get_rows(self, v_rid):
        """ Get rows from DB.

        Args:
            `v_rid` [list of int]
                A list of row Ids.

        Returns:
            `subarray`  [numpy.ndarray]
                rows specified by `v_rid`.
        """
        nrows = len(v_rid)
        resarr = np.ndarray((nrows, self.ncols), self.dtype)
        for i in range(nrows):
            resarr[i, :] = self.get_row(v_rid[i])
        return resarr

    def set_rows(self, v_rid, arr):
        """ Set rows of DB

        Args:
            `v_rid` [list of int]
                A list of row Ids.
            `arr`   [numpy.ndarray]
                Rows specified by `v_rid`.

        Returns: N/A
        """
        nrows = len(v_rid)
        for i in range(nrows):
            self.set_row(v_rid[i], arr[i, :])

    def get_row(self, rid):
        """ Get a row.

        Args:
            `rid`       [int]           A single row Id.

        Returns:
            `subarray`  [numpy.ndarray] Row vector specified by `rid`.
        """
        return np.ndarray(self.ncols, self.dtype,
                          self._storage.get(pack(PACK_NUM_TYPE, rid)))

    def set_row(self, rid, arr):
        """ Set a row

        Args:
            `rid`   [int]           A single row Id.
            `arr`   [numpy.ndarray] Row vector specified by `rid`.

        Returns: N/A
        """
        return self._storage.set(pack(PACK_NUM_TYPE, rid), arr.data)

    def set_db_attr(self, key, val):
        """ Set DB attribute.

        Args:
            `key`   [str]   Name of the attribute.
            `val`   [str, int or 1-row numpy.ndarray]
                Value of the attribute.
                The following attribute type are supported:
                    'nda': numpy.ndarray
                    'int': int
                    'str': string

        Returns: N/A
        """
        if type(val) is np.ndarray:
            dtype_str = self._get_dtype_name(val.dtype)
            self.set_db_attr(key + "_dtype", dtype_str)
            return self._storage.set(key, TSTR_NDARRAY +
                                     val.tostring())
        elif type(val) is int:
            return self._storage.set(key, TSTR_INT +
                                     pack(PACK_NUM_TYPE, val))
        elif type(val) is str:
            return self._storage.set(key, TSTR_STR + val)
        else:
            raise TypeError('Unsupported attribute type: %s' % str(type(val)))

    def get_db_attr(self, key):
        """ Set DB attribute.

        Args:
            `key`   [str]
                Name of the attribute.

        Returns:
            `val`   [str, int or 1-row numpy.ndarray]
                Value of the attribute.
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
            return unpack(PACK_NUM_TYPE,
                          rawval[len(TSTR_INT):])[0]
        # string
        elif rawval[:len(TSTR_STR)] == TSTR_STR:
            return rawval[len(TSTR_STR):]
        # unknown type
        else:
            raise('Unknown attribute type: %s' % rawval[:8])

    @classmethod
    def fromndarray(cls, arr, dbpath, dbtype=DEFAULT_DTYPE):
        """ Construct `DBArray` from `ndarray`.

        Args:
            `arr`       [numpy.ndarray] The source `ndarray`.
            `dbpath`    [str]   Path of the database.
            `dbtype`    [str]   Type of the database.

        Returns:
            `dba`       [DBArray]
        """
        dba = DBArray(dbpath, dbtype)
        dba.set_dtype(arr.dtype)
        dba.set_shape(arr.shape)
        for rid in range(arr.shape[0]):
            dba.set_row(rid, arr[rid, :])
        return dba

    def tondarray(self):
        """ Load data to `ndarray` from `DBArray`.

        Args: N/A

        Returns:
            `arr`   [numpy.ndarray]
        """
        return self.get_rows(range(self.nrows))

    @classmethod
    def _get_dtype_name(cls, dtype):
        """ Get the name of data type
        """
        if dtype is None:
            return 'None'
        elif type(dtype) is type:
            return dtype.__name__
        elif type(dtype) is np.dtype:
            return dtype.name
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
            return np.dtype(dtype_str)

    def _loadinfo(self):
        """ Load information from DB
        """
        self.nrows = unpack(PACK_NUM_TYPE, self._storage.get('nrows'))[0]
        self.ncols = unpack(PACK_NUM_TYPE, self._storage.get('ncols'))[0]
        self.dtype = self._gen_dtype(self._storage.get('dtype'))

    @classmethod
    def _parse_key_core(cls, key, stop=0):
        """ Parse the provided key into list.

        Args:
            `key`   [int, long, slice or list]
                Valide key can be of type `int`, `long` or `slice`.
            `stop`  [int]
                Upper bound of the keys.

        Returns:
            `keylist`   [list or None]
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
            logging.warning('Invalid key: %s' % str(key))
            return None

    @classmethod
    def _parse_key_for_array(cls, key, stop_rows=0, stop_cols=0):
        if type(key) is tuple:
            if len(key) == 0:
                # no key
                logging.error("Error: invalid syntax!")
            elif len(key) == 1:
                return cls._parse_key_core(key[0], stop_rows), None
            elif len(key) == 2:
                return (cls._parse_key_core(key[0], stop_rows),
                        cls._parse_key_core(key[1], stop_cols))
            else:
                # bad key
                logging.error("Error: too many indices!")
        else:
            return cls._parse_key_core(key, stop_rows), None

        # return invalid value by default
        return None, None
