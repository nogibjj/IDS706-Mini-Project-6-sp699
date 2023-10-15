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

    print(combined_results)

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

    print(result)

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

    print(results)

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

    print(results)

    # Return results
    return results

if __name__ == "__main__":
    join()
    aggregation()
    sorting()
    complex_query()
