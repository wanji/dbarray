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

import leveldb


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
