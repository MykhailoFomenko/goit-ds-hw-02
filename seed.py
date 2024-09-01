import sqlite3
from contextlib import contextmanager


database = './goit-ds-hw-02.db'


@contextmanager
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = sqlite3.connect(db_file)
    yield conn
    conn.rollback()
    conn.close()


def create_users(conn, project):
    sql = '''
    INSERT INTO users(fullname,email) VALUES(?,?);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, project)
        conn.commit()
    # except sqlite3.Error as e:
    #     print(e)
    finally:
        cur.close()


def create_status(conn, project):
    sql = '''
    INSERT INTO status(name) VALUES(?);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, project)
        conn.commit()
    # except sqlite3.Error as e:
    #     print(e)
    finally:
        cur.close()


def create_tasks(conn, project):
    sql = '''
    INSERT INTO tasks(title,description,status_id,user_id) VALUES(?,?,?,?);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, project)
        conn.commit()
    # except sqlite3.Error as e:
    #     print(e)
    finally:
        cur.close()


if __name__ == '__main__':
    with create_connection(database) as conn:
        users = [("Yuriy Boyko", "yura228@gmail.com"), ("George Chambers", "iceman1337@ukr.net"), ("Antony Kark", "ironman123@gmail.com")]  # З незрозумілих для мене причин, мені не вдається використати бібліотеку "Fake", тому я сподіваюсь, нічого страшного, якщо я використаю написані мною данні для заповнення баз даних
        statuses = [('new',), ('in progress',), ('completed',)]
        tasks = [("Clean a car", "Clean an old Toyota Camry", 2, 3), ("Go to the store", "Go to 7/11 and buy a milk", 3, 1), ("Fly to NewYork", "Buy a plain ticket and fly to NewYork sity", 1, 2)]

        for i in users:
            create_users(conn, i)

        for i in statuses:
            create_status(conn, i)
        
        for i in tasks:
            create_tasks(conn, i)