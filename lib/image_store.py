# -*- coding: utf-8 -*-
import os
import multiprocessing
import sqlite3 as sqlite

db_lock = multiprocessing.Lock()

class MBTilesWriter(object):
    SCHEME = '''
        CREATE TABLE tiles(
            zoom_level integer, tile_column integer, tile_row integer, tile_data blob,
            UNIQUE(zoom_level, tile_column, tile_row) ON CONFLICT REPLACE);
       
        CREATE TABLE metadata (name text, value text, UNIQUE(name) ON CONFLICT REPLACE);
    '''
    
    PRAGMAS = '''
        PRAGMA journal_mode = off;
        PRAGMA synchronous = 0;
        PRAGMA busy_timeout = 10000;
    '''

    def __init__(self, path):
        need_init = not os.path.exists(path)
        self.path = path
        if need_init:
            self.conn.executescript(self.SCHEME)

    _conn = None

    @property
    def conn(self):
        if self._conn is None:
            conn  = self._conn = sqlite.connect(self.path)
            conn.executescript(self.PRAGMAS)
        return self._conn
    
    def write(self, data, tile_x, tile_y, level):
        tile_y = 2 ** level - tile_y - 1
        s = buffer(data)
        with db_lock:
            conn = self.conn
            conn.execute('''
                INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?,?,?,?)''',
                (level, tile_x, tile_y, s))

    def close(self):
        conn = self.conn
        conn.commit()
        conn.close()


class FilesWriter(object):
    def __init__(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)
        self.path = path

    def _get_tile_file_name(self, tile_x, tile_y, level):
        filename = '%s_%s_%s' % (level, tile_y, tile_x)
        filename = os.path.join(self.path, filename)
        return filename
    
    def write(self, data, tile_x, tile_y, level):
        filename = self._get_tile_file_name(tile_x, tile_y, level)
        with open(filename, 'w') as f:
            f.write(data)

    def remove(self, tile_x, tile_y, level):
        filename = self._get_tile_file_name(tile_x, tile_y, level)
        if os.path.exists(filename):
            os.remove(filename)

    def close(self):
        pass
