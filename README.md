![Complex Query](https://github.com/nogibjj/IDS706-Mini-Project-6-sp699/actions/workflows/main.yml/badge.svg)
# IDS-706-Data-Engineering :computer:

## Mini Project 6 :page_facing_up: 

## :ballot_box_with_check: Requirements
* Design a complex SQL query involving joins, aggregation, and sorting</br>
* Provide an explanation for what the query is doing and the expected results</br>

## :ballot_box_with_check: To-do List
* __Understanding database connection__: To understand join, aggregatin, sort functions in SQLite.</br>

## :ballot_box_with_check: Dataset
* `Books and Authors`
  - The data contains information about books and its authors.</br>
* `Brief Description`</br>
  1) __booksDB.db__
    - `id`: integer (1-8 assigned)
    - `title`: text (title of books)
    - `type`: text (original or translated)
    - `author_id`: integer (11-15 assgined)
![image](https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/58cb2665-201c-44ec-b3c1-dbcadb681c1f)</br>
  2) __authorsDB.db__
    - `id`: integer (11-15 assigned, author id)
    - `first_name`: text (first name of authors)
    - `last_name`: text (last name of authors)
![image](https://github.com/nogibjj/IDS706-Mini-Project-6-sp699/assets/143478016/130cc8e5-fc17-4f24-9fc3-d80794177996)</br>

## :ballot_box_with_check: In progress
__`Step 1`__ : Set up the environment to install multiple Python versions in GitHub Actions.
- `requirements.txt`: Add `requests`(version=2.31.0).</br>
<img src="https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/357d739d-8e38-4b59-88cd-ec59b07cb023.png" width="180" height="190"/></br>
- `main.yml`: Set up the environment for the SQL database in the __main.yml__ file.  </br>
<img src="https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/4adbb162-e58e-41f8-aeed-dcd4c932cd08.png" width="360" height="660"/></br>
- `Makefile`: Include the functions for install, test, lint, and format to automate the build. </br>
<img src="https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/57b59ef1-1a4b-447a-838e-d3e795980e86.png" width="430" height="230"/></br>
- `Dockerfile`: Create a Dockerfile that builds an image to create a container in Codespace.</br>
- `devcontainer.json`: Define and set up a development environment.

__`Step 2`__ : Add SQL complex queries to the __libarary/query.py__ file. Extract the CSV files and then transform the database files.</br>
* `library/extract.py`</br>
```Python
# Extract csv file through link
import requests

def extract_one(url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/books.csv", 
            file_path="Data/books.csv"):
    with requests.get(url, timeout=10) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path

def extract_two(url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/authors.csv", 
            file_path="Data/authors.csv"):
    with requests.get(url, timeout=10) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path
```
* `library/transform.py`</br>
```Python
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
        CREATE TABLE authors (
            id INTEGER,
            first_name TEXT,
            last_name TEXT
        )
    """
    )
    # insert
    c.executemany(
        """
        INSERT INTO authors (
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
```

* `library/query.py`</br>
```Python
import sqlite3

def join():
    # Connect to 'booksDB' database
    books_conn = sqlite3.connect("booksDB.db")
    books_cursor = books_conn.cursor()

    # Connect to 'authorsDB' database
    authors_conn = sqlite3.connect("authorsDB.db")
    authors_cursor = authors_conn.cursor()

    # Execute SQL queries (books database)
    books_query = """
    SELECT b.id, b.title, b.author_id
    FROM books b
    ORDER BY b.id;
    """
    books_cursor.execute(books_query)

    # Execute SQL queries (authors database)
    authors_query = """
    SELECT c.id, c.first_name, c.last_name
    FROM authors c;
    """
    authors_cursor.execute(authors_query)

    # Fetch results
    books_results = books_cursor.fetchall()
    authors_results = authors_cursor.fetchall()

    # Close connections
    books_conn.close()
    authors_conn.close()

    # Combine results
    combined_results = []

    for book_result in books_results:
        book_id, book_title, author_id = book_result
        author_info = [author for author in authors_results if author[0] == author_id]
        if author_info:
            author_first_name, author_last_name = author_info[0][1], author_info[0][2]
            combined_results.append((book_id, book_title, author_first_name, author_last_name))

    return combined_results

def aggregation():
    conn = sqlite3.connect("booksDB.db")
    cursor = conn.cursor()

    # Execute SQL query
    query = """
    SELECT type, COUNT(*) as original_count
    FROM books
    WHERE type = 'original'
    GROUP BY type;
    """
    cursor.execute(query)

    # Fetch result
    result = cursor.fetchone()

    # Close connection
    conn.close()

    return result

def sorting():
    # Connect to the database
    conn = sqlite3.connect("booksDB.db")
    cursor = conn.cursor()

    # Execute SQL query
    query = """
    SELECT id, title
    FROM books
    ORDER BY title;
    """
    cursor.execute(query)

    # Fetch results
    results = cursor.fetchall()

    # Close connection
    conn.close()

    return results

def complex_query():
    # Connect to 'booksDB' database
    conn = sqlite3.connect("booksDB.db")
    cursor = conn.cursor()

    # Execute SQL query (Select Translated Books and Sort by Title)
    query = """
    SELECT id, title
    FROM books
    WHERE type = 'translated'
    ORDER BY title ASC;
    """
    cursor.execute(query)

    # Fetch results
    results = cursor.fetchall()

    # Close connection
    conn.close()

    # Return results
    return results

if __name__ == "__main__":
    join()
    aggregation()
    sorting()
    complex_query()
```

* `main.py`</br>
```Python
from library.extract import extract_one, extract_two
from library.transform import load_database_one, load_database_two
from library.query import join, aggregation, sorting, complex_query

if __name__ == "__main__":
    extract_one(url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/books.csv", 
            file_path="Data/books.csv")
    extract_two(url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/authors.csv", 
            file_path="Data/authors.csv")
    load_database_one()
    load_database_two()
    print(join())
    aggregation()
    sorting()
    complex_query()
```

* `test_main.py`</br>
```Python
# test main.py
from library.extract import extract_one, extract_two
from library.transform import load_database_one, load_database_two
from library.query import join, aggregation, sorting, complex_query

import sqlite3

def test_extract_one():
    result = extract_one(url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/books.csv", 
            file_path="Data/books.csv")
    assert result is not None

def test_extract_two():
    result = extract_two(url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/authors.csv", 
            file_path="Data/authors.csv")
    assert result is not None

def test_load_database_one():
    data = load_database_one()
    if data:
        print("Database 'booksDB.db' loading successful:")
        for row in data:
            print(row)
    else:
        print("Failed to load the database")

def test_load_database_two():
    data = load_database_two()
    if data:
        print("Database 'authorsDB.db' loading successful:")
        for row in data:
            print(row)
    else:
        print("Failed to load the database")

def test_join():
    # Execute the join function
    results = join()

    # Compare the results with expected results
    expected_results = [(1, 'Time to Grow Up!', 'Ellen', 'Writer'), (2, 'Your Trip', 'Yao', 'Dou'), (3, 'Lovely Love', 'Donald', 'Brain'), (4, 'Dream Your Life', 'Ellen', 'Writer'), (5, 'Oranges', 'Olga', 'Savelieva'), (6, 'Your Happy Life', 'Yao', 'Dou'), (7, 'Applied AI', 'Jack', 'Smart'), (8, 'My Last Book', 'Ellen', 'Writer')]
    assert results == expected_results, "Test failed: The results do not match the expected output."

    print("Test passed: join function works correctly.")

def test_aggregation():
    # Execute the aggregation function
    result = aggregation()

    # Compare the result with expected result
    expected_result = ('original', 4)  # Expected result: ('original', 4)

    assert result == expected_result, "Test failed: The results do not match the expected output."

    print("Test passed: The aggregation function works correctly.")

def test_sorting():
    # Execute the sorting function
    results = sorting()

    # Compare the results with expected result
    expected_result = [(7, 'Applied AI'), (4, 'Dream Your Life'), (3, 'Lovely Love'), (8, 'My Last Book'), (5, 'Oranges'), (1, 'Time to Grow Up!'), (6, 'Your Happy Life'), (2, 'Your Trip')]  # Expected result

    assert results == expected_result, "Test failed: The results do not match the expected output."

    print("Test passed: The sorting function works correctly.")

def test_complex_query():
    results = complex_query()

    expected_result = [(7, 'Applied AI'), (5, 'Oranges'), (6, 'Your Happy Life'), (2, 'Your Trip')]

    assert results == expected_result, "Test failed: The results do not match the expected output."

    print("Test passed: The complex query function works correctly.")

if __name__ == "__main__":
    test_extract_one()
    test_extract_two()
    test_load_database_one()
    test_load_database_two()
    test_join()
    test_aggregation()
    test_sorting()
    test_complex_query()
```

__`Step 3`__ : Verify if SQL runs correctly.
* __Test__ </br>
![image](https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/7f5c833b-798e-40db-87b7-100e5b31660a)</br>
* __Lint__ </br>
<img src="https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/fd1233f0-1853-4cb3-84dd-09035ece1b97.png" width="700" height="100"/></br>
* __Format__ </br>
<img src="https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/0c11445c-adbc-42d9-86a1-15a51ee72b88.png" width="480" height="100"/></br>

## Description of Query
* __`join`__ </br>
The "join" query is a query that performs an INNER JOIN, using the "author_id" from the "booksDB" database and the "id" from "authorsDB" to retrieve the names of the authors of the books.</br>
![image](https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/569ecb77-45b0-49f4-92be-bbc39604fa5e)</br>
* __`aggregation`__ </br>
The "aggregation" query is a query that gathers all the books categorized as "original" by their type and calculates the count of such books.</br>
![image](https://github.com/nogibjj/IDS706-Mini-Project-6-sp699/assets/143478016/dbb0fc49-ac5f-4be6-9977-6864adf4ebf7)</br>
* __`sorting`__ </br>
The "sorting" query is a query that sorts the titles of books in alphabetical order in ascending order, and it displays the id and title of the books.</br>
![image](https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/9130215b-e64e-4ef1-b674-03236e4d1a38)</br>
* __`complex_query`__ </br>
The "complex_query" query is a query that utilizes the functionalities of the aggregation and sorting queries to retrieve the names of books with the "translated" type, sorting them in alphabetical order in ascending order, and displaying their id and title.</br>
![image](https://github.com/nogibjj/IDS706-Mini-Project-5-sp699/assets/143478016/455a9e90-7371-46ee-adc7-628f375c892e)</br>
