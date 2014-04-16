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
This is main interface of DBArray.
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
        self.storage.set('dtype', str(dtype))

    def loadinfo(self):
        """ Load information from DB
        """
        self.nrows = struct.unpack(PACK_NUM_TYPE, self.storage.get('nrows'))[0]
        self.ncols = struct.unpack(PACK_NUM_TYPE, self.storage.get('ncols'))[0]
        self.dtype = eval('np.' + self.storage.get('dtype'))

    @classmethod
    def parse_key(cls, key, stop):
        """ parse key
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
        """ get values
        """
        v_rid = None
        v_cid = None
        if type(key) is not tuple:
            v_rid = self.parse_key(key, self.nrows)
            if None == v_rid:
                perr("Error: key must be a tuple or integer\n")
                return None
        else:
            if len(key) == 0:
                perr("Error: invalid syntax\n")
                return None
            elif len(key) > 2:
                perr("Error: too many indices\n")
                return None
            elif len(key) == 1:
                v_rid = self.parse_key(key[0], self.nrows)
                v_cid = None
            elif len(key) == 2:
                v_rid = self.parse_key(key[0], self.nrows)
                v_cid = self.parse_key(key[1], self.ncols)

        rows = self.get_rows(v_rid)
        if None != v_cid:
            rows = rows[:, v_cid]
        return rows

    def get_rows(self, v_rid):
        """ Get a row
        """
        nrows = len(v_rid)
        resarr = np.ndarray((nrows, self.ncols), self.dtype)
        for i in range(nrows):
            resarr[i, :] = self.get_row(v_rid[i])
        return resarr

    def set_rows(self, v_rid, arr):
        """ Get a row
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
        """ Get a row
        """
        self.storage.set(struct.pack(PACK_NUM_TYPE, rid), arr.data)

    def __del__(self):
        """ Destroy
        """
        print "Destroy"

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
