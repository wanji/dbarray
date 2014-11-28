#!/usr/bin/env python
# coding: utf-8

#########################################################################
#########################################################################

"""
   File Name: storage.py
      Author: Wan Ji
      E-mail: wanji@live.com
  Created on: Thu Mar 20 12:28:31 2014 CST
"""
DESCRIPTION = """
The backend storage engine of DBArray
"""

import os
import leveldb
import lmdb
import logging


class Storage(object):
    """ Basic storage
    """

    def __init__(self):
        pass

    def set(self, key, val):
        """ Set `key` to `val`
        """
        raise Exception('Unimplemented method in %s: set(%s, %s)' %
                        self.__class__.__name__, str(key), str(val))

    def get(self, key):
        """ Get value of `key`
        """
        raise Exception('Unimplemented method in %s: get(%s)' %
                        self.__class__.__name__, str(key))

    @classmethod
    def is_valid(cls, dbpath):
        raise Exception('Unimplemented method in %s: is_valid(%s)' %
                        cls.__name__, str(dbpath))


class StorageLevelDB(Storage):
    """ Storage using LevelDB as backend.
    """

    def __init__(self, dbpath):
        Storage.__init__(self)
        self.hl_db = leveldb.LevelDB(dbpath, write_buffer_size=2**30)

    def __del__(self):
        del self.hl_db

    def set(self, key, val):
        """ Set `key` to `val`
        """
        self.hl_db.Put(key, val)

    def get(self, key):
        """ Get value of `key`
        """
        return self.hl_db.Get(key)

    @classmethod
    def is_valid(cls, dbpath):
        for item in os.listdir(dbpath):
            if item == 'CURRENT':
                return True
        return False


class StorageLMDB(Storage):
    """ Storage using LevelDB as backend.
    """
    DB_MAP = {}

    def __init__(self, dbpath, map_size=2**40):
        Storage.__init__(self)
        abspath = os.path.abspath(dbpath)
        if abspath not in StorageLMDB.DB_MAP:
            StorageLMDB.DB_MAP[abspath] = \
                lmdb.open(dbpath, map_size=map_size, sync=False)
        try:
            StorageLMDB.DB_MAP[abspath].stat()
        except lmdb.Error:
            StorageLMDB.DB_MAP[abspath] = \
                lmdb.open(dbpath, map_size=map_size, sync=False)
        self.env = StorageLMDB.DB_MAP[abspath]

    def __del__(self):
        pass

    def set(self, key, val):
        """ Set `key` to `val`
        """
        with self.env.begin(write=True) as txt:
            txt.put(key, val)

    def get(self, key):
        """ Get value of `key`
        """
        loop = True
        while loop:
            try:
                with self.env.begin() as txt:
                    val = txt.get(key)
                loop = False
            except lmdb.BadRSlotError as err:
                logging.warning(err.message)
        return val

    @classmethod
    def is_valid(cls, dbpath):
        for item in os.listdir(dbpath):
            if item.endswith('.mdb'):
                return True
        return False


class StorageRedis(Storage):
    """ Storage using Redis as backend.
    """

    def __init__(self):
        Storage.__init__(self)
        raise Exception('Unimplemented class.')

    def set(self, key, val):
        """ Set `key` to `val`
        """
        raise Exception('Unimplemented method in %s: set(%s, %s)' %
                        self.__class__.__name__, str(key), str(val))

    def get(self, key):
        """ Get value of `key`
        """
        raise Exception('Unimplemented method in %s: set(%s, %s)' %
                        self.__class__.__name__, str(key))
