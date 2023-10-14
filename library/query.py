import sqlite3

def join():
    # 데이터베이스 연결
    conn = sqlite3.connect("booksDB.db")
    cursor = conn.cursor()

    # SQL 쿼리 실행
    query = """
    SELECT b.id, b.title, a.first_name, a.last_name
    FROM books b
    INNER JOIN authors a
    ON b.author_id = a.id
    ORDER BY b.id;
    """
    cursor.execute(query)

    # 결과 가져오기
    results = cursor.fetchall()

    # 연결 닫기
    conn.close()

    return results

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