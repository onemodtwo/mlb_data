# -*- coding: utf-8 -*-
"""
Provides utility to connect to PostgreSQL database and return cursor
"""

from sys import path
path.append('./db_utils/')
from connect import connect
