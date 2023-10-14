# Transform .csv file to .db file
import sqlite3
import csv


# load the csv file and transform it to db file
def load_database_one(dataset="Data/books.csv", encoding="utf-8"):
    subset_data = csv.reader(open(dataset, newline="", encoding=encoding), delimiter=",")
    # skips the header of csv
    next(subset_data)
    conn = sqlite3.connect("booksDB.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS books")
    c.execute(
        """
        CREATE TABLE books (
            id INTEGER,
            title TEXT,
            type TEXT,
            author_id INTEGER
        )
    """
    )
    # insert
    c.executemany(
        """
        INSERT INTO books (
            id,
            title,
            type,
            author_id
            ) 
            VALUES (?, ?, ?, ?)""",
        subset_data,
    )
    conn.commit()
    conn.close()
    return "booksDB.db"

def load_database_two(dataset="Data/authors.csv", encoding="utf-8"):
    subset_data = csv.reader(open(dataset, newline="", encoding=encoding), delimiter=",")
    # skips the header of csv
    next(subset_data)
    conn = sqlite3.connect("authorsDB.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS authors")
    c.execute(
        """
        CREATE TABLE subset (
            id INTEGER,
            first_name TEXT,
            last_name TEXT
        )
    """
    )
    # insert
    c.executemany(
        """
        INSERT INTO subset (
            id,
            first_name,
            last_name
            ) 
            VALUES (?, ?, ?)""",
        subset_data,
    )
    conn.commit()
    conn.close()
    return "authorsDB.db"