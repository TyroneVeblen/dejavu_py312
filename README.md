### Original repository address: https://github.com/worldveil/dejavu
The original author hasn't updated it in a while and doesn't seem to accept pull requests anymore. So I've updated it, it may be buggy, but at least the registration and recognition works!

### Support for sqlite3
I added a database_sqlite class, which now supports sqlite3.

This is how he uses it.

```
config = {
    "database_type":"sqlite",
    "database": {
        "database": "data\\song_finger.db",#Replace it with your database address, or the address of the database you envisage (it will be created automatically)
        "check_same_thread":False #I'm actually not sure if this is helpful, but it might help if you want to multithread?
    }
}
```
### MYSQL TO SQLITE python script
If you want to migrate from mysql to sqlite, try this script.
```
sqlite_database=sqlite3.connect("data\\song_finger.db",check_same_thread=False,)#Change it to your own.
mysql_database=pymysql.connect(host="localhost", user="root", passwd="1234567890", database="song_finger",
                                    charset="utf8mb4")#Change it to your own.
mysql_cur = mysql_database.cursor()
get_ids_SQL = "SELECT song_id FROM songs"
mysql_cur.execute(get_ids_SQL)
sqlite_cur=sqlite_database.cursor()
result = mysql_cur.fetchall()
for row in result:
    song_id = int(row[0])
    get_fingerprints_sql="SELECT hex(hash),song_id,offset FROM fingerprints where song_id=%s"
    get_song_info_sql="SELECT * FROM songs where song_id=%s"
    insert_fingerprints_sql="INSERT INTO fingerprints VALUES(?,?,?)"
    insert_song_info_sql="INSERT INTO songs VALUES(?,?,?)"
    mysql_cur.execute(get_song_info_sql, (song_id,))
    song_info=mysql_cur.fetchone()
    sqlite_cur.execute(insert_song_info_sql, song_info)
    sqlite_database.commit()
    mysql_cur.execute(get_fingerprints_sql, (song_id,))
    fingerprints = mysql_cur.fetchall()
    sqlite_cur.execute("BEGIN TRANSACTION;")
    try:
        for fingerprints_row in fingerprints:
            hash, song_id, offset = fingerprints_row
            hash = str(hash).lower()
            sqlite_cur.execute(insert_fingerprints_sql, (hash, song_id, offset))
        sqlite_database.commit()
    except Exception as e:
        print(e)
        sqlite_database.rollback()
    finally:
        print(f"Now insert song_id is{song_id}")
        try:
            sqlite_cur.execute("END TRANSACTION;")
        except Exception as e:
            pass
```

