import sqlite3
from itertools import zip_longest
import copy
from dejavu.database import Database
import queue
import numpy as np

class SQLiteDatabase(Database):
    """
    SQLite implementation of the Database interface.
    """

    type = "sqlite"

    # tables
    FINGERPRINTS_TABLENAME = "fingerprints"
    SONGS_TABLENAME = "songs"

    # fields
    FIELD_FINGERPRINTED = "fingerprinted"

    # creates
    CREATE_FINGERPRINTS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS {FINGERPRINTS_TABLENAME} (
            {Database.FIELD_HASH} TEXT NOT NULL,
            {Database.FIELD_SONG_ID} INTEGER NOT NULL,
            {Database.FIELD_OFFSET} INTEGER NOT NULL,
            UNIQUE ({Database.FIELD_HASH}, {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET}),
            FOREIGN KEY ({Database.FIELD_SONG_ID}) REFERENCES {SONGS_TABLENAME}({Database.FIELD_SONG_ID})
            ON DELETE CASCADE
        );
    """

    CREATE_SONGS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS {SONGS_TABLENAME} (
            {Database.FIELD_SONG_ID} INTEGER PRIMARY KEY AUTOINCREMENT,
            {Database.FIELD_SONGNAME} TEXT NOT NULL,
            {FIELD_FINGERPRINTED} INTEGER DEFAULT 0,
            {Database.FIELD_FILE_SHA1} BLOB NOT NULL
        );
    """

    # inserts (ignores duplicates)
    INSERT_FINGERPRINT = f"""
        INSERT OR IGNORE INTO {FINGERPRINTS_TABLENAME} 
        ({Database.FIELD_HASH}, {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET}) 
        VALUES (?, ?, ?);
    """

    INSERT_SONG = f"""
        INSERT INTO {SONGS_TABLENAME} 
        ({Database.FIELD_SONGNAME}, {Database.FIELD_FILE_SHA1}) 
        VALUES (?, ?);
    """

    # selects
    SELECT = f"""
        SELECT {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET} 
        FROM {FINGERPRINTS_TABLENAME} 
        WHERE {Database.FIELD_HASH} = ?;
    """

    SELECT_MULTIPLE = f"""
        SELECT {Database.FIELD_HASH}, {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET}
        FROM {FINGERPRINTS_TABLENAME}
        WHERE {Database.FIELD_HASH} IN ({','.join(['?'] * 1000)});
    """

    SELECT_ALL = f"""
        SELECT {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET} FROM {FINGERPRINTS_TABLENAME};
    """

    SELECT_SONG = f"""
        SELECT {Database.FIELD_SONGNAME}, {Database.FIELD_FILE_SHA1} 
        FROM {SONGS_TABLENAME} 
        WHERE {Database.FIELD_SONG_ID} = ?;
    """

    SELECT_NUM_FINGERPRINTS = f"""
        SELECT COUNT(*) AS n FROM {FINGERPRINTS_TABLENAME};
    """

    SELECT_UNIQUE_SONG_IDS = f"""
        SELECT COUNT(DISTINCT {Database.FIELD_SONG_ID}) AS n 
        FROM {SONGS_TABLENAME} 
        WHERE {FIELD_FINGERPRINTED} = 1;
    """

    SELECT_SONGS = f"""
        SELECT {Database.FIELD_SONG_ID}, {Database.FIELD_SONGNAME}, {Database.FIELD_FILE_SHA1} 
        FROM {SONGS_TABLENAME} 
        WHERE {FIELD_FINGERPRINTED} = 1;
    """

    # drops
    DROP_FINGERPRINTS = f"DROP TABLE IF EXISTS {FINGERPRINTS_TABLENAME};"
    DROP_SONGS = f"DROP TABLE IF EXISTS {SONGS_TABLENAME};"

    # update
    UPDATE_SONG_FINGERPRINTED = f"""
        UPDATE {SONGS_TABLENAME} SET {FIELD_FINGERPRINTED} = 1 WHERE {Database.FIELD_SONG_ID} = ?;
    """

    # delete
    DELETE_UNFINGERPRINTED = f"""
        DELETE FROM {SONGS_TABLENAME} WHERE {FIELD_FINGERPRINTED} = 0;
    """

    def __init__(self, **options):
        super(SQLiteDatabase, self).__init__()
        options["isolation_level"]=None
        self.conn=sqlite3.connect(**options)
        self.custom_cursor=self.conn.cursor()
        self.cursor = cursor_factory(**options)


    def after_fork(self):
        # SQLite does not need special handling for forking
        pass

    def setup(self):
        with self.cursor() as cur:
            cur.execute(self.CREATE_SONGS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def empty(self):
        with self.cursor() as cur:
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_SONGS)
        self.setup()

    def delete_unfingerprinted_songs(self):
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_songs(self):
        with self.cursor() as cur:
            return cur.execute(self.SELECT_UNIQUE_SONG_IDS).fetchone()["n"]

    def get_num_fingerprints(self):
        with self.cursor() as cur:
            return cur.execute(self.SELECT_NUM_FINGERPRINTS).fetchone()["n"]

    def set_song_fingerprinted(self, sid):
        with self.cursor() as cur:
            cur.execute(self.UPDATE_SONG_FINGERPRINTED, (sid,))

    def get_songs(self):
        with self.cursor() as cur:
            for row in cur.execute(self.SELECT_SONGS):
                yield dict(row)

    def get_song_by_id(self, sid):
        with self.cursor() as cur:
            return dict(cur.execute(self.SELECT_SONG, (sid,)).fetchone())

    def insert(self, hash, sid, offset):
        offset=int(offset)
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, (hash, sid, offset))

    def insert_song(self, songname, file_hash):
        with self.cursor() as cur:
            cursor = cur.execute(self.INSERT_SONG, (songname, file_hash))
            return cursor.lastrowid

    def query(self, hash):
        query = self.SELECT_ALL if hash is None else self.SELECT
        with self.cursor() as cur:
            for row in cur.execute(query, (hash,) if hash else ()):
                yield tuple(row)

    def insert_hashes(self, sid, hashes):
        with self.cursor() as cur:
            cur.execute("BEGIN")
            for hash, offset in hashes:
                offset = int(offset)
                cur.execute(self.INSERT_FINGERPRINT, (hash, sid, offset))

    def return_matches(self, hashes, batch_size=1000):
        mapper = {h: o for h, o in hashes}
        hash_keys = list(mapper.keys())

        # Process in batches
        for i in range(0, len(hash_keys), batch_size):
            batch_values = hash_keys[i:i + batch_size]
            placeholders = ','.join(['?'] * len(batch_values))  # Create the correct number of placeholders
            query = f"""
                SELECT {Database.FIELD_HASH}, {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET}
                FROM {self.FINGERPRINTS_TABLENAME}
                WHERE {Database.FIELD_HASH} IN ({placeholders});
            """

            with self.cursor() as cur:
                for row in cur.execute(query, batch_values):
                    hash, sid, offset = row
                    yield (sid, offset - mapper[hash])

    def __getstate__(self):
        return (self._options,)

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in zip_longest(fillvalue=fillvalue, *args))


def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)
    return cursor


class Cursor(object):
    """
    Establishes a connection to the SQLite database and returns an open cursor.

    ```python
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
    ```
    """
    _cache = queue.Queue(maxsize=5)

    def __init__(self, **options):
        super(Cursor, self).__init__()
        # Attempt to retrieve an existing connection from the cache
        try:
            conn = self._cache.get_nowait()
        except queue.Empty:
            # Establish a new SQLite connection if none are available in the cache
            conn = sqlite3.connect(**options)
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.isolation_level = None  # Enable autocommit mode in SQLite

    @classmethod
    def clear_cache(cls):
        cls._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # Rollback on error, commit otherwise
        if extype:
            self.conn.rollback()
        else:
            self.conn.commit()
        # Close the cursor and put the connection back in the cache
        self.cursor.close()
        try:
            self._cache.put_nowait(self.conn)
        except queue.Full:
            self.conn.close()


