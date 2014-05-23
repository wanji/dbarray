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
DBArray class.
"""

import os
import sys
import struct

import numpy as np

import storage

DBTYPE = {
    "leveldb":  storage.StorageLevelDB,
    "redis":    storage.StorageRedis,
}

# storing the number in `long long` type
PACK_NUM_TYPE = 'q'
TSTR_NDARRAY  = 'nda'
TSTR_INT      = 'int'
TSTR_LEN      = len(TSTR_INT)

def perr(msg):
    """ Print error message.
    """

    sys.stderr.write("%s" % msg)
    sys.stderr.flush()


def pinfo(msg):
    """ Print information message.
    """

    sys.stdout.write("%s" % msg)
    sys.stdout.flush()


class DBArray(object):
    """ Array stored in database.
    """

    def __init__(self, dbpath, dbtype='leveldb'):
        """ Init.
        """
        self.nrows = -1
        self.ncols = -1
        self.dtype = None

        is_exists = os.path.exists(dbpath)

        self.storage = DBTYPE[dbtype](dbpath)

        # load information from existing DB
        if is_exists:
            self.loadinfo()
        else:
            self.set_shape((self.nrows, self.ncols))
            self.set_dtype(self.dtype)

    def set_shape(self, shape):
        """ Set shape of DBArray
        """
        (self.nrows, self.ncols) = shape
        self.storage.set('nrows', struct.pack(PACK_NUM_TYPE, shape[0]))
        self.storage.set('ncols', struct.pack(PACK_NUM_TYPE, shape[1]))

    def set_dtype(self, dtype):
        """ Set shape of DBArray
        """
        self.dtype = dtype
        if dtype is None:
            self.storage.set('dtype', 'None')
        elif type(dtype) is type:
            self.storage.set('dtype', dtype.__name__)
        elif type(dtype) is str:
            self.storage.set('dtype', dtype)
        else:
            raise Exception('Unrecognized data type: %s' % str(dtype))

    def loadinfo(self):
        """ Load information from DB
        """
        self.nrows = struct.unpack(PACK_NUM_TYPE, self.storage.get('nrows'))[0]
        self.ncols = struct.unpack(PACK_NUM_TYPE, self.storage.get('ncols'))[0]
        raw_dtype = self.storage.get('dtype')
        if raw_dtype == 'None':
            self.dtype = None
        else:
            self.dtype = eval('np.' + raw_dtype)

    @classmethod
    def parse_key(cls, key, stop=0):
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
            return range(0 if key.start is None else 0,
                         stop if key.stop is None else key.stop,
                         1 if key.step is None else key.step)
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
            v_rid = self.parse_key(key, self.nrows)
            if None == v_rid:
                perr("Error: key must be a tuple or integer\n")
                return None
        else:
            if len(key) == 0:
                # bad key
                perr("Error: invalid syntax\n")
                return None
            elif len(key) > 2:
                # bad key
                perr("Error: too many indices\n")
                return None
            elif len(key) == 1:
                # get rows
                v_rid = self.parse_key(key[0], self.nrows)
                v_cid = None
            elif len(key) == 2:
                # get rows and then select cols
                v_rid = self.parse_key(key[0], self.nrows)
                v_cid = self.parse_key(key[1], self.ncols)

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
                          self.storage.get(struct.pack(PACK_NUM_TYPE, rid)))

    def set_row(self, rid, arr):
        """ Set a row
        """
        return self.storage.set(struct.pack(PACK_NUM_TYPE, rid), arr.data)

    def set_db_attr(self, key, val):
        """
        Set attribute
        """
        if type(val) is np.ndarray:
            return self.storage.set(key, TSTR_NDARRAY +
                                    val.tostring())
        elif type(val) is int:
            return self.storage.set(key, TSTR_INT +
                                    struct.pack(PACK_NUM_TYPE, val))
        else:
            raise('Unsupported attribute type: %s' % str(type(val)))

    def get_db_attr(self, key):
        """
        Set attribute
        """
        rawval = self.storage.get(key)
        if rawval[:len(TSTR_NDARRAY)] == TSTR_NDARRAY:
            return np.fromstring(rawval[len(TSTR_NDARRAY):])
        elif rawval[:len(TSTR_INT)] == TSTR_INT:
            return struct.unpack(PACK_NUM_TYPE,
                                 rawval[len(TSTR_INT):])[0]
        else:
            raise('Unknown attribute type: %s' % rawval[:8])

    def __del__(self):
        """ Destroy
        """
        pass

    ################################################################
    #
    ################################################################

    @classmethod
    def fromndarray(cls, arr, dbpath):
        """ Construct DBArray from ndarray
        """
        dba = DBArray(dbpath)
        dba.set_dtype(arr.dtype)
        dba.set_shape(arr.shape)
        for rid in range(arr.shape[0]):
            dba.set_row(rid, arr[rid, :])
        return dba

    def tondarray(self):
        """ Convert to ndarray
        """
        return self.get_rows(range(self.nrows))
