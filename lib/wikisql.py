# coding: utf-8
import csv
import gzip

# partially from https://github.com/jamesmishra/mysqldump-to-csv

def is_insert(line):
    """
    Returns true if the line begins a SQL insert statement.
    """
    return line.startswith('INSERT INTO') or False


def get_values(line):
    """
    Returns the portion of an INSERT statement containing values
    """
    return line.partition('` VALUES ')[2]


def values_sanity_check(values):
    """
    Ensures that values from the INSERT statement meet basic checks.
    """
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True

def parse_values(values):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )

    for reader_row in reader:
        for column in reader_row:
            # If our current string is empty...
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            # If our string starts with an open paren
            if column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False
                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:
                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    yield latest_row
                    latest_row = []
                # If we're beginning a new row, eliminate the
                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]
            # Add our column to the row we're working on.
            latest_row.append(column)
        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and
        # the close paren.
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            yield latest_row


def iterate_rows(filename):
    for line in gzip.open(filename):
        if is_insert(line):
            values = get_values(line)
            assert values_sanity_check(values)
            for row in parse_values(values):
                yield row


def extract_coords(rows):
    for row in rows:
        assert 11 == len(row), repr(row)
        id_, page_id, globe, primary, lat, lon, dim, type_, name, country, region = row
        if globe == 'earth' and primary == '1':
            flat = float(lat)
            flon = float(lon)
            if flat == 0 or flon == 0:
                continue
            if flat < -85.06 or flat > 85.06 or flon < -180 or flon > 180:
                continue
            if flat == int(flat) and flon == int(flon):
                continue
            yield flon, flat, int(page_id)


def iterate_coords(in_file):
    return extract_coords(iterate_rows(in_file))


def extract_image_page_ids(rows):
    for row in rows:
        page_id, page_namespace, page_title = row[:3]
        if page_namespace == '6':
            basename, _, ext = page_title.rpartition('.')
            if basename and ext.lower() == 'jpg':
                yield int(page_id)


def iterate_image_pages(in_file):
    return extract_image_page_ids(iterate_rows(in_file))
