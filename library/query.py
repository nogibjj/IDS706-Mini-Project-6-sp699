import sqlite3

def join():
    # 'booksDB' 데이터베이스 연결
    books_conn = sqlite3.connect("booksDB.db")
    books_cursor = books_conn.cursor()

    # 'authorsDB' 데이터베이스 연결
    authors_conn = sqlite3.connect("authorsDB.db")
    authors_cursor = authors_conn.cursor()

    # SQL 쿼리 실행 (books 데이터베이스)
    books_query = """
    SELECT b.id, b.title, b.author_id
    FROM books b
    ORDER BY b.id;
    """
    books_cursor.execute(books_query)

    # SQL 쿼리 실행 (authors 데이터베이스)
    authors_query = """
    SELECT c.id, c.first_name, c.last_name
    FROM authors c;
    """
    authors_cursor.execute(authors_query)

    # 결과 가져오기
    books_results = books_cursor.fetchall()
    authors_results = authors_cursor.fetchall()

    # 연결 닫기
    books_conn.close()
    authors_conn.close()

    # 결과 조합
    combined_results = []

    for book_result in books_results:
        book_id, book_title, author_id = book_result
        author_info = [author for author in authors_results if author[0] == author_id]
        if author_info:
            author_first_name, author_last_name = author_info[0][1], author_info[0][2]
            combined_results.append((book_id, book_title, author_first_name, author_last_name))
    
    # 결과 확인을 위한 print 문
    print(combined_results)

    return combined_results

def aggregation():
    conn = sqlite3.connect("booksDB.db")
    cursor = conn.cursor()

    # SQL 쿼리 실행
    query = """
    SELECT type, COUNT(*) as original_count
    FROM books
    WHERE type = 'original'
    GROUP BY type;
    """
    cursor.execute(query)

    # 결과 가져오기
    result = cursor.fetchone()

    # 연결 닫기
    conn.close()

    return result

def sorting():
    # 데이터베이스 연결
    conn = sqlite3.connect("booksDB.db")
    cursor = conn.cursor()

    # SQL 쿼리 실행
    query = """
    SELECT title
    FROM books
    ORDER BY title;
    """
    cursor.execute(query)

    # 결과 가져오기
    results = cursor.fetchall()

    # 연결 닫기
    conn.close()

    return results

if __name__ == "__main__":
    join()