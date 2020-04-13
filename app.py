import psycopg2
from datetime import datetime
from typing import List, Optional
from pprint import pprint

DB_NAME = ''
USER = ''
PASS_WORD = ''


def create_db() -> None:
    with psycopg2.connect(database=DB_NAME, user=USER, password=PASS_WORD) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS Student(
                id SERIAL NOT NULL,
                name VARCHAR(100) NOT NULL,
                gpa NUMERIC(10, 2),
                birth TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (id)
                );

                CREATE TABLE IF NOT EXISTS Course(
                id SERIAL NOT NULL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
                );

                CREATE TABLE IF NOT EXISTS Student_Course(
                student_id INT NOT NULL REFERENCES Student(id),
                course_id INT NOT NULL REFERENCES Course(id),
                PRIMARY KEY (student_id, course_id)
                );
                ''')


def get_students(course_id: int):
    with psycopg2.connect(database=DB_NAME, user=USER, password=PASS_WORD) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT Student.name
                FROM Student_Course
                INNER JOIN Student ON Student_Course.student_id=Student.id
                WHERE Student_Course.course_id=%s;
                ''',
                (course_id,)
            )
            return cursor.fetchall()


def add_students(course_id: int, students: List[dict]) -> None:
    with psycopg2.connect(database=DB_NAME, user=USER, password=PASS_WORD) as connection:
        with connection.cursor() as cursor:
            def add_one(student: dict):
                cursor.execute(
                    '''
                    INSERT INTO Student (name, gpa, birth)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                    ''',
                    (student.get('name'), student.get('gpa'), student.get('birth'), )
                )
                return cursor.fetchone()[0]
            cursor.execute('SELECT EXISTS (SELECT id FROM Course WHERE Course.id=%s)', (course_id, ))
            if not cursor.fetchone()[0]:
                print('Сначала создайте курс')
                return
            for student in students:
                _id = add_one(student)
                cursor.execute(
                    'INSERT INTO Student_Course (student_id, course_id) VALUES (%s, %s);',
                    (_id, course_id)
                )


def add_student(student: dict) -> None:
    with psycopg2.connect(database=DB_NAME, user=USER, password=PASS_WORD) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO Student (name, gpa, birth) VALUES (%s, %s, %s);',
                (student.get('name'), student.get('gpa', 0), student.get('birth', datetime(1990, 1, 1)), )
            )


def get_student(student_id: int):
    with psycopg2.connect(database=DB_NAME, user=USER, password=PASS_WORD) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM Student WHERE id=%s;', (student_id,))
            return cursor.fetchone()


def add_course(course_name: str) -> int:
    with psycopg2.connect(database=DB_NAME, user=USER, password=PASS_WORD) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                INSERT INTO Course (name)
                VALUES (%s)
                RETURNING id;
                ''',
                (course_name, )
            )
            return cursor.fetchone()[0]


if __name__ == "__main__":
    create_db()
    students_names = ['Вася', 'Петя', 'Ольга', 'Дарья', 'Игорь', 'Иван', 'Олежка']
    students = []
    for name in students_names:
        students.append({
            'name': name,
        })
    course_name = 'python'
    course_id = add_course(course_name)
    add_students(course_id, students)
    print(f'Студенты курса {course_name}-{course_id}:')
    pprint(get_students(course_id))
    add_student({
        'name': 'Владимир',
        'gpa': 2.0,
        'birth': datetime.now()
    })
    print(get_student(3))
