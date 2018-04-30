# -*- coding: utf-8 -*-

from configparser import ConfigParser
import sqlalchemy
import os


def config(filename, section):
    parser = ConfigParser()  # create a parser
    parser.read(filename)    # read config file

    # get section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'
                        .format(section, filename))

    return db


def connect(section, filename=os.path.expanduser('~') + '/path/to/database.ini'):
    """Connect to the PostgreSQL database server and return cursor

    """
    conn = None
    try:
        # read connection parameters and build connection url
        params = config(filename, section)
        db_url = 'postgresql://{}:{}@{}/{}'.format(params['user'],
                                                   params['password'],
                                                   params['host'],
                                                   params['dbname'])

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        engine = sqlalchemy.create_engine(db_url)
        conn = engine.connect()
        if params.get('search_path'):
            conn.execute('SET search_path TO ' + params['search_path'])

        # display the PostgreSQL database server version
        db_version = list(conn.execute('SELECT version()'))[0]
        print('PostgreSQL database version:\n{}'.format(db_version))

        return conn

    except (Exception, sqlalchemy.exc.DatabaseError) as error:
        print(error)
        return
