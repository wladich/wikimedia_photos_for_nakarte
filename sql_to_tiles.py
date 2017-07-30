#!/usr/bin/env python
# coding: utf-8
import os
import tempfile
import pysqlite2.dbapi2 as sqlite
from lib import wikisql
import pyproj
from lib.image_store import MBTilesWriter
from cStringIO import StringIO
from array import array
import Image, ImageDraw

proj_wgs84 = pyproj.Proj('+init=EPSG:4326')
proj_gmerc = pyproj.Proj('+init=EPSG:3857')


vector_level = 11

symbol_radius = 5
_symbol = None
symbol_color = (255, 0, 255)


class PointsStorage(object):
    def __init__(self, temp_dir):
        self.tmp_file = tempfile.NamedTemporaryFile(dir=temp_dir)
        self.db = sqlite.connect(self.tmp_file.name)
        self.db.executescript('''
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            CREATE TABLE point (x NUMERIC, y NUMERIC, UNIQUE(x, y) ON CONFLICT IGNORE);
            CREATE INDEX idx_point_x ON point(x);
            CREATE INDEX idx_point_y ON point(y);
        ''')

    def add(self, x, y):
        self.db.execute('INSERT INTO point (x, y) VALUES (?, ?)', (x, y))

    def get_points_in_bbox(self, minx, miny, maxx, maxy):
        return self.db.execute('SELECT x, y FROM point WHERE ? < x AND ? < y AND x < ?  AND y < ?',
                               (minx, miny, maxx, maxy))

    def commit(self):
        self.db.commit()
        self.db.execute('ANALYZE;')


def load_points_to_tmp_db(in_file, temp_dir):
    points = PointsStorage(temp_dir)
    for lon, lat in wikisql.iterate_coords(in_file):
        x, y = pyproj.transform(proj_wgs84, proj_gmerc, lon, lat)
        points.add(x, y)
    points.commit()
    return points


def get_symbol(r):
    global _symbol
    if _symbol is None:
        r = symbol_radius
        dest_size = r * 2 + 1
        q = 4
        im = Image.new('L', (dest_size * q, dest_size * q), 0)
        draw = ImageDraw.Draw(im)
        draw.ellipse([0, 0, 2 * r * q, 2 * r * q], fill=255)
        del draw
        _symbol = im.resize((dest_size, dest_size), Image.ANTIALIAS)
    return _symbol


def get_tile_extents(x, y, z):
    max_coord = 20037508.342789244
    tile_size = 2 * max_coord / (1 << z)
    return (x * tile_size - max_coord, y * tile_size - max_coord, tile_size)


def tile_index_from_tms((x, y, z)):
    y = (2 ** z) - 1 - y
    return x, y, z


def draw_tile(points, tile_x, tile_y, tile_z):
    r = symbol_radius
    tile_min_x, tile_min_y, tile_size = get_tile_extents(tile_x, tile_y, tile_z)
    margin = tile_size / 256 * symbol_radius
    points_bbox = (
        tile_min_x - margin,
        tile_min_y - margin,
        tile_min_x + tile_size + margin,
        tile_min_y + tile_size + margin
    )

    im = Image.new('L', (256, 256), 0)
    marker = get_symbol(r)
    has_points = False
    for x, y in points.get_points_in_bbox(*points_bbox):
        has_points = True
        pix_x = (x - tile_min_x) / tile_size * 256
        pix_y = (y - tile_min_y) / tile_size * 256
        pix_y = 256 - pix_y
        pix_x = int(pix_x)
        pix_y = int(pix_y)
        im.paste(255, (pix_x - r, pix_y - r, pix_x + r + 1, pix_y + r + 1), mask=marker)

    if has_points:
        #TODO: make paletted images
        im2 = Image.new('RGBA', im.size)
        im2.paste(symbol_color + (255,), (0, 0), mask=im)
        f = StringIO()
        im2.save(f, 'PNG')
        return f.getvalue()
    else:
        return None


def make_vector_tile(points, tile_x, tile_y, tile_z):
    offset = 5000
    extent = 65535 - 2 * offset
    tile_min_x, tile_min_y, tile_size = get_tile_extents(tile_x, tile_y, tile_z)

    margin = tile_size / 256 * symbol_radius
    points_bbox = (
        tile_min_x - margin,
        tile_min_y - margin,
        tile_min_x + tile_size + margin,
        tile_min_y + tile_size + margin
    )

    ar = array('H')
    for x, y in points.get_points_in_bbox(*points_bbox):
        x = (x - tile_min_x) / tile_size * extent + offset
        y = (1 - (y - tile_min_y) / tile_size) * extent + offset
        ar.append(int(round(x)))
        ar.append(int(round(y)))
    return ar.tostring()


def iterate_tiles(points):
    queue = [(0, 0, 0)]
    while queue:
        tile_index = queue.pop()
        x, y, z = tile_index
        if z < vector_level:
            tile_data = draw_tile(points, *tile_index)
            if tile_data:
                queue.extend([
                    (x * 2, y * 2, z + 1),
                    (x * 2 + 1, y * 2, z + 1),
                    (x * 2, y * 2 + 1, z + 1),
                    (x * 2 + 1, y * 2 + 1, z + 1),
                ])
        else:
            tile_data = make_vector_tile(points, *tile_index)
        if not tile_data:
            continue
        yield tile_data, tile_index


def main(in_file, out_file):
    if os.path.exists(out_file):
        raise Exception('File "%s" exists' % out_file)
    tmp_dir = os.path.abspath(os.path.dirname(out_file))
    points = load_points_to_tmp_db(in_file, tmp_dir)

    writer = MBTilesWriter(out_file)
    for tile_data, tile_index in iterate_tiles(points):
        tile_index = tile_index_from_tms(tile_index)
        writer.write(tile_data, *tile_index)
    writer.close()


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print 'Usage: %s /path/to/commonswiki-latest-geo_tags.sql.gz commons-wiki-tiles.db' % 'sql_to_tiles.py'
    main(*sys.argv[1:])