import sqlite3

def dictionary_row_factory(cursor, row):
    """Dictionary based row factory function (see
     https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.row_factory)

    This is very useful to compose json responses from query results
    :param cursor: sqlite3 cursor
    :param row: a single row
    :return: a dictionary (column name: value) representing the row
    """

    row_as_dict = {}
    for column_index, column in enumerate(cursor.description):
        # column[0] contains the column name
        column_name = column[0]
        row_as_dict[column_name] = row[column_index]

    return row_as_dict


class DBHelper(object):
    """
    This is a simple DB Helper to be used on Flask WebService projects

    It provides (android fashion) facility methods for query, insert, update and delete methods

    Typical usage:

    app = Flask(__name__)
    db = DBHelper('database.db', g, dbg=True)

    db.insert(..)
    db.query(..)
    db.delete(..)
    """

    def __init__(self, db_name, g_flask, dbg=False, row_factory=None):
        """
        The 'Constructor'
        :param db_name: relative path for database. It will be used for DB connection
        :param g_flask: the Flask global container (to hold database reference)
        :param dbg: controls debug information
        :param row_factory: the sqlite3 row factory function (see
        https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.row_factory)
                            If not specified, a dictionary factory will be used
        :return: initialized db_helper reference
        """
        self._database = db_name
        self._g = g_flask
        self._dbg = dbg
        if row_factory:
            self.row_factory
        else:
            self.row_factory = dictionary_row_factory

    def _get_db(self):
        """
        Internal method for db connection
        :return: db reference
        """
        db = getattr(self._g, '_database', None)
        if db is None:
            if self._dbg:
                print 'open()'
                print 'Opening database'
            db = self._g._database = sqlite3.connect(self._database)

        return db

    def query(self, table, projection=None, selection='', selection_args=None, sort_order=''):
        """
        Facility method for db query
        Typical usage: rows = db.query('TABLE_NAME', ['id', 'col1', 'col2', 'col4'], 'id =?', (str(2),))
        Equivalent to: SELECT id, col1, col2, col4 from TABLE_NAME WHERE ID =?,  params=2
        :param table: the table to be queried
        :param projection: the list of columns to be returned by this query (use None for *)
        :param selection: the selection expression (where clause)
        :param selection_args: the arguments for select expression
        :param sort_order: sort order expression
        :return: list of rows created using the provided row_factory (see __init__)
        """
        db = self._get_db()
        db.row_factory = self.row_factory

        query = ''
        columns = '*'
        if projection:
            columns = ''
            for column in projection:
                columns += column+','
            columns = columns[:-1]

        query = ' '.join(['SELECT', columns, 'from', table, 'WHERE', selection, sort_order]) if selection \
            else ' '.join(['SELECT', columns, 'from', table, sort_order])

        if self._dbg:
            print 'query()'
            print query + ' params=%s' % selection_args

        cursor = db.execute(query, selection_args) if selection_args else db.execute(query)
        rows = cursor.fetchall()
        cursor.close()

        return rows

    def insert(self, table, projection=None, values=None):
        """
        Facility method for db insertion
        Typical usage: rows = db.insert('TABLE_NAME', ['id', 'col1', 'col2', 'col4'], (v1, v2, v3, v4))
        Equivalent to: INSERT INTO TABLE_NAME (id, col1, col2, col4) VALUES (?, ?, ?, ?),  params=(v1, v2, v3, v4)
        :param table: the table to insert data into
        :param projection: the list of columns whose values were specified. May be omitted if values for all column were
                           provided
        :param values: tuple of values to insert into db (should match the projection)
        :return: 'OK'
        """
        db = self._get_db()

        query = ''
        columns = ''                                       # (Col1, Col2, Col4)

        if projection:
            columns = '('
            for column in projection:
                columns += column+','
            columns = columns[:-1]+')'

        wild_cards = ''                                    # (=?, =?, =?)
        if values:
            wild_cards = '('
            for v in values:
                wild_cards += '?,'
            wild_cards = wild_cards[:-1]+')'

        query = ' '.join(['INSERT INTO', table, columns, 'VALUES', wild_cards]) if projection \
            else ' '.join(['INSERT INTO', table, columns, 'VALUES', wild_cards])

        if self._dbg:
            print 'insert()'
            print query

        cursor = db.execute(query, values)

        db.commit()
        cursor.close()

        return 'OK'

    def update(self, table, projection, values, selection, selection_args):
        """
        Facility method for db updating
        Typical usage: rows = db.update('TABLE_NAME', fields, values, 'ID =?', [id])
        Equivalent to: UPDATE TABLE_NAME SET c1=? ,c2=? ,c3=?  WHERE ID =? ,  params=[v1, v2, v3] [id]
        :param table: table whose row will be updated
        :param projection: the columns to be updated
        :param values: the values to update (should match the projection)
        :param selection: the selection expression (where clause)
        :param selection_args: the arguments for select expression
        :return: 'OK'
        """
        db = self._get_db()

        query = ''
        columns = 'SET '                                    # SET Col1 =?, Col2 =?, Col4 =?
        for column in projection:
            columns += column+'=? ,'
        columns = columns[:-1]

        wild_cards = '('                                    # (=?, =?, =?)
        for v in values:
            wild_cards += '?,'
        wild_cards = wild_cards[:-1]+')'

        query = ' '.join(['UPDATE', table, columns, 'WHERE', selection])

        if self._dbg:
            print 'update()'
            print query, values+selection_args

        cursor = db.execute(query, values+selection_args)
        db.commit()
        cursor.close()

        return 'OK'

    def delete(self, table, selection, selection_args):
        """
        Facility method for db exclusion
        Typical usage: result = db.delete('TABLE_NAME', 'ID =?', (str(id),))
        Equivalent to: DELETE FROM TABLE_NAME WHERE ID =?  ,  params=(v1, )
        :param table: table whose row will be deleted
        :param selection: the selection expression (where clause)
        :param selection_args: the arguments for select expression
        :return: 'OK'
        """
        db = self._get_db()

        query = ' '.join(['DELETE FROM', table, 'WHERE', selection])
        if self._dbg:
            print 'delete()'
            print query, selection_args

        cursor = db.execute(query, selection_args)
        db.commit()
        cursor.close()

        return 'OK'

    def close(self):
        """
        Facility method to close the db connection
        :return: None
        """
        if self._dbg:
            print 'close()'
            print 'closing db'

        db = self._get_db()
        db.close()
