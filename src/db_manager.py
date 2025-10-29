
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Tuple
from config import DB_CONFIG



class DBManager:
    """
    Класс для работы с базой данных PostgreSQL.
    Обеспечивает создание таблиц, загрузку данных и выполнение аналитических запросов.
    """

    def __init__(self):
        """Инициализация соединения с БД"""
        self.connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )

    def create_tables(self):
        """
        Создать таблицы в БД:
        - employers: данные о компаниях
        - vacancies: данные о вакансиях (с FK на employers)
        """
        create_employers = """
        CREATE TABLE IF NOT EXISTS employers (
            id SERIAL PRIMARY KEY,
            employer_id BIGINT UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            url VARCHAR(255),
            vacancies_count INT
        );
        """

        create_vacancies = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            vacancy_id BIGINT UNIQUE NOT NULL,
            employer_id BIGINT NOT NULL,
            title VARCHAR(255) NOT NULL,
            salary_from INT,
            salary_to INT,
            currency VARCHAR(10),
            url VARCHAR(255),
            FOREIGN KEY (employer_id) REFERENCES employers (employer_id)
                ON DELETE CASCADE
        );
        """

        with self.connection.cursor() as cursor:
            cursor.execute(create_employers)
            cursor.execute(create_vacancies)
        self.connection.commit()

    def add_employer(self, employer: Dict):
        """
        Добавить работодателя в таблицу employers.
        При конфликте (по employer_id) — обновляет данные.

        Args:
            employer (Dict): словарь с данными работодателя из API hh.ru
        """
        query = """
        INSERT INTO employers (employer_id, name, url, vacancies_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (employer_id) DO UPDATE
        SET name = EXCLUDED.name,
            url = EXCLUDED.url,
            vacancies_count = EXCLUDED.vacancies_count;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (
                employer['id'],
                employer['name'],
                employer['alternate_url'],
                employer.get('open_vacancies', 0)
            ))
        self.connection.commit()

    def add_vacancy(self, vacancy: Dict, employer_id: int):
        """
        Добавить вакансию в таблицу vacancies.
        При конфликте (по vacancy_id) — пропускает запись.

        Args:
            vacancy (Dict): словарь с данными вакансии из API hh.ru
            employer_id (int): ID работодателя
        """
        salary = vacancy.get('salary')
        salary_from = salary.get('from') if salary else None
        salary_to = salary.get('to') if salary else None
        currency = salary.get('currency') if salary else None

        query = """
        INSERT INTO vacancies (vacancy_id, employer_id, title, salary_from, salary_to, currency, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vacancy_id) DO NOTHING;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (
                vacancy['id'],
                employer_id,
                vacancy['name'],
                salary_from,
                salary_to,
                currency,
                vacancy['alternate_url']
            ))
        self.connection.commit()

    def get_companies_and_vacancies_count(self) -> List[Tuple]:
        """
        Получить список всех компаний и количество вакансий у каждой.

        Returns:
            List[Tuple]: список кортежей (название компании, количество вакансий)
        """
        query = """
        SELECT e.name, COUNT(v.id) as vacancy_count
        FROM employers e
        LEFT JOIN vacancies v ON e.employer_id = v.employer_id
        GROUP BY e.name
        ORDER BY vacancy_count DESC;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def get_all_vacancies(self) -> List[Dict]:
        """
        Получить все вакансии с указанием:
        - названия компании
        - названия вакансии
        - зарплаты (от и до)
        - ссылки на вакансию

        Returns:
            List[Dict]: список словарей с данными вакансий
        """
        query = """
        SELECT
            e.name as company_name,
            v.title as vacancy_title,
            v.salary_from,
            v.salary_to,
            v.currency,
            v.url
        FROM vacancies v
        JOIN employers e ON v.employer_id = e.employer_id;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def get_avg_salary(self) -> float:
        """
        Получить среднюю зарплату по всем вакансиям (по полю salary_from).

        Returns:
            float: средняя зарплата (0.0, если данных нет)
        """
        query = "SELECT AVG(salary_from) as avg_salary FROM vacancies WHERE salary_from IS NOT NULL;"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return float(result[0]) if result[0] else 0.0

    def get_vacancies_with_higher_salary(self) -> List[Dict]:
        """
        Получить список вакансий, у которых зарплата выше средней по всем вакансиям.

        Returns:
            List[Dict]: список словарей с данными вакансий (компания, название, зарплата, ссылка)
        """
        avg_salary = self.get_avg_salary()
        query = f"""
        SELECT
            e.name as company_name,
            v.title as vacancy_title,
            v.salary_from,
            v.salary_to,
            v.currency,
            v.url
        FROM vacancies v
        JOIN employers e ON v.employer_id = e.employer_id
        WHERE v.salary_from > {avg_salary};
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict]:
        """
        Получить список вакансий, в названии которых содержится ключевое слово.

        Args:
            keyword (str): ключевое слово для поиска (без учёта регистра)

        Returns:
            List[Dict]: список словарей с данными вакансий (компания, название, зарплата, ссылка)
        """
        query = """
        SELECT
            e.name as company_name,
            v.title as vacancy_title,
            v.salary_from,
            v.salary_to,
            v.currency,
            v.url
        FROM vacancies v
        JOIN employers e ON v.employer_id = e.employer_id
        WHERE LOWER(v.title) LIKE LOWER(%s);
        """
        search_pattern = f"%{keyword}%"

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (search_pattern,))
            return [dict(row) for row in cursor.fetchall()]
        