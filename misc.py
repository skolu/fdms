import leveldb

db = leveldb.DB()
db.open('level.db')
with db.iterator() as it:
    it.seek(b'l')
    for key, value in it:
        print(key)
        print(value)

