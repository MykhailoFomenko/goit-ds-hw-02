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


def select_tasks_for_user(conn, user_id):
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT title FROM tasks WHERE user_id=?", (user_id,))
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def select_tasks_for_user_with_status(conn, user_id, status_id):
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT title FROM tasks WHERE user_id=? AND status_id=?", (user_id,status_id))
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def update_status_task(conn, title, new_status):
    sql = '''
    UPDATE tasks
    SET status_id = ?
    WHERE title = ?
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, (new_status, title))
        conn.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()


def select_users_without_any_task(conn):
    sql = '''
    SELECT fullname
    FROM users
    WHERE id NOT IN (
     SELECT user_id
     FROM tasks);
    '''
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def insert_new_task_to_user(conn, new_task):
    sql = '''
    INSERT INTO tasks(title, description, status_id, user_id) VALUES(?,?,?,?)
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, new_task) #Виглядаєб як стандартне додавання нового рядку до таблиці tasks, фле оскільки ми вказуємо user_id, ми додаємо це завдання саме конкретному користувачу
        conn.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()


def select_all_not_ended_tasks(conn):
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT title FROM tasks WHERE status_id !=3")
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def delete_task(conn, task_id):
    sql = 'DELETE FROM tasks WHERE id=?'
    cur = conn.cursor()
    try:
        cur.execute(sql, (task_id, ))
        conn.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()


def select_user_by_email(conn, email):
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT fullname FROM users WHERE email LIKE '%{email}'")
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def update_username(conn, old_name, new_name):
    sql = '''
    UPDATE users
    SET fullname = ?
    WHERE fullname = ?
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, (new_name, old_name))
        conn.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()


def group_tasks_by_status(conn):
    sql = '''
    SELECT COUNT(title) as tasks, status_id
        FROM tasks
        GROUP BY status_id;
    '''
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def select_task_by_email_domen(conn, domen):
    sql = f'''
    SELECT a.title, b.email
        FROM tasks as a
        JOIN users as b ON b.id = a.user_id
        WHERE b.email LIKE "%{domen}"
    '''
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def select_tasks_without_desc(conn):
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT title FROM tasks WHERE description IS NULL")
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def users_with_progress_status(conn):
    sql = '''
    SELECT a.fullname as users, b.title as task
        FROM users as a
        INNER JOIN tasks as b ON b.user_id = a.id
        WHERE b.status_id = 2
        GROUP BY b.id
    '''
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def users_and_count_of_their_tasks(conn):
    sql = '''
    SELECT a.fullname as users, COUNT(b.title) as task
        FROM users as a
        LEFT JOIN tasks as b ON b.user_id = a.id
        GROUP BY a.id
    '''
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
    return rows


with create_connection(database) as conn:
    print(select_tasks_for_user(conn, 1))
    print(select_tasks_for_user_with_status(conn, 1, 3))
    update_status_task(conn, "Clean a car", 1)
    print(select_users_without_any_task(conn))
    insert_new_task_to_user(conn, ("Any task", None, 1, 1))
    print(select_all_not_ended_tasks(conn))
    delete_task(conn, 2)
    print(select_user_by_email(conn, "@gmail.com"))
    update_username(conn, "Antony Kark", "Antony Stark")
    print(group_tasks_by_status(conn))
    print(select_task_by_email_domen(conn, "@ukr.net"))
    print(select_tasks_without_desc(conn))
    print(users_with_progress_status(conn))
    print(users_and_count_of_their_tasks(conn))