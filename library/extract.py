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

