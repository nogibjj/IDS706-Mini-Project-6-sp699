import sqlite3
from library.extract import extract_one, extract_two
from library.transform import load_database_one, load_database_two
from library.query import join, aggregation, sorting

if __name__ == "__main__":
    extract_one(url="https://github.com/suim-park/Mini-Project-6/blob/main/Data/books.csv", 
            file_path="Data/books.csv")
    extract_two(url="https://github.com/suim-park/Mini-Project-6/blob/main/Data/authors.csv", 
            file_path="Data/authors.csv")
    load_database_one()
    load_database_two()
    join()
    aggregation()
    sorting()