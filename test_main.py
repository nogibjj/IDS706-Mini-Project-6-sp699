# test main.py
from library.extract import extract_one, extract_two
from library.transform import load_database_one, load_database_two
from library.query import join, aggregation, sorting, complex_query

import sqlite3


def test_extract_one():
    result = extract_one(
        url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/books.csv",
        file_path="Data/books.csv",
    )
    assert result is not None


def test_extract_two():
    result = extract_two(
        url="https://github.com/suim-park/Mini-Project-6/raw/main/Data/authors.csv",
        file_path="Data/authors.csv",
    )
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
    expected_results = [
        (1, "Time to Grow Up!", "Ellen", "Writer"),
        (2, "Your Trip", "Yao", "Dou"),
        (3, "Lovely Love", "Donald", "Brain"),
        (4, "Dream Your Life", "Ellen", "Writer"),
        (5, "Oranges", "Olga", "Savelieva"),
        (6, "Your Happy Life", "Yao", "Dou"),
        (7, "Applied AI", "Jack", "Smart"),
        (8, "My Last Book", "Ellen", "Writer"),
    ]
    assert (
        results == expected_results
    ), "Test failed: The results do not match the expected output."

    print("Test passed: join function works correctly.")


def test_aggregation():
    # Execute the aggregation function
    result = aggregation()

    # Compare the result with expected result
    expected_result = ("original", 4)  # Expected result: ('original', 4)

    assert (
        result == expected_result
    ), "Test failed: The results do not match the expected output."

    print("Test passed: The aggregation function works correctly.")


def test_sorting():
    # Execute the sorting function
    results = sorting()

    # Compare the results with expected result
    expected_result = [
        (7, "Applied AI"),
        (4, "Dream Your Life"),
        (3, "Lovely Love"),
        (8, "My Last Book"),
        (5, "Oranges"),
        (1, "Time to Grow Up!"),
        (6, "Your Happy Life"),
        (2, "Your Trip"),
    ]  # Expected result

    assert (
        results == expected_result
    ), "Test failed: The results do not match the expected output."

    print("Test passed: The sorting function works correctly.")


def test_complex_query():
    results = complex_query()

    expected_result = [
        (7, "Applied AI"),
        (5, "Oranges"),
        (6, "Your Happy Life"),
        (2, "Your Trip"),
    ]

    assert (
        results == expected_result
    ), "Test failed: The results do not match the expected output."

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
