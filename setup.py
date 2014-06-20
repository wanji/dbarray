#!/usr/bin/env python
# coding: utf-8
"""
   File Name: setup.py
      Author: Wan Ji
      E-mail: wanji@live.com
  Created on: Tue 18 Mar 2014 12:03:18 PM CST
 Description:
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='dbarray',
      version='0.1',
      author='WAN Ji',
      author_email='wanji@live.com',
      package_dir={'dbarray': 'src'},
      packages=['dbarray'],
      url='http://pypi.python.org/pypi/dbarray/',
      license='LICENSE.txt',
      description='.',
      long_description=open('README.md').read(),
      install_requires=[
          "numpy      >= 1.7.0",
          "leveldb    >= 0.192",
      ],
      )
