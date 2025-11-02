import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Tuple
from config import DB_CONFIG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class DBManager:
    """
    Класс для работы с PostgreSQL: создание БД, таблиц, загрузка данных и выполнение запросов.
    Обеспечивает:
    - автоматическое создание БД и таблиц;
    - добавление работодателей и вакансий;
    - аналитические запросы к данным.
    """

    def __init__(self):
        """Инициализация подключения к системной БД 'postgres' для операций DDL"""
        self.connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            database='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def create_database(self):
        """
        Создать БД, если она не существует.
        Использует системную БД 'postgres' для выполнения CREATE DATABASE.
        """
        db_name = DB_CONFIG['database']
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(f'CREATE DATABASE {db_name}')
                logger.info(f"БД {db_name} создана.")
            else:
                logger.info(f"БД {db_name} уже существует.")

    def reconnect_to_target_db(self):
        """
        Переподключиться к целевой БД (после её создания).
        Обновляет self.connection для работы с данными.
        """
        self.connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )

    def create_tables(self):
        """
        Создать таблицы employers и vacancies, если они не существуют.
        Обеспечивает целостность данных через FOREIGN KEY.
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
        logger.info("Таблицы employers и vacancies созданы (если не существовали).")

    def add_employer(self, employer: Dict) -> None:
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

    def add_vacancy(self, vacancy: Dict, employer_id: int) -> None:
        """
        Добавить вакансию в таблицу vacancies.
        Обрабатывает ошибки и пропущенные поля.

        Args:
            vacancy (Dict): словарь с данными вакансии из API hh.ru
            employer_id (int): ID работодателя
        """
        # Проверка обязательных полей
        if 'id' not in vacancy or 'name' not in vacancy:
            logger.warning(f"Пропущена вакансия: отсутствуют обязательные поля {vacancy}")
            return

        salary = vacancy.get('salary')
        salary_from = salary.get('from') if salary else None
        salary_to = salary.get('to') if salary else None
        currency = salary.get('currency') if salary else None
        url = vacancy.get('alternate_url', 'N/A')  # Запасной вариант для URL

        query = """
        INSERT INTO vacancies (vacancy_id, employer_id, title, salary_from, salary_to, currency, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vacancy_id) DO NOTHING;
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (
                    vacancy['id'],
                    employer_id,
                    vacancy['name'],
                    salary_from,
                    salary_to,
                    currency,
                    url
                ))
            self.connection.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении вакансии {vacancy['id']}: {e}")
            self.connection.rollback()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получить список компаний и количество их вакансий.

        Returns:
            List[Tuple[str, int]]: список кортежей (название компании, количество вакансий)
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
        Получить все вакансии с деталями.

        Returns:
            List[Dict]: список словарей с полями:
                - company_name
                - vacancy_title
                - salary_from
                - salary_to
                - currency
                - url
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
        Вычислить среднюю зарплату по всем вакансиям (по полю salary_from).

        Returns:
            float: средняя зарплата (0.0, если данных нет)
        """
        query = """
        SELECT AVG(salary_from) as avg_salary
        FROM vacancies
        WHERE salary_from IS NOT NULL;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return float(result[0]) if result[0] else 0.0

    def get_vacancies_with_higher_salary(self) -> List[Dict]:
        """
        Получить вакансии с зарплатой выше средней.

        Returns:
            List[Dict]: список вакансий (с теми же полями, что и в get_all_vacancies)
        """
        avg_salary = self.get_avg_salary()
        if avg_salary == 0.0:
            return []

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
        WHERE v.salary_from > %s
        ORDER BY v.salary_from DESC;
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (avg_salary,))
            return [dict(row) for row in cursor.fetchall()]

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict]:
        """
        Поиск вакансий по ключевому слову в названии.

        Args:
            keyword (str): ключевое слово для поиска

        Returns:
            List[Dict]: список подходящих вакансий (с полями как в get_all_vacancies)
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
        WHERE LOWER(v.title) LIKE LOWER(%s)
        ORDER BY e.name, v.title;
        """
        search_pattern = f"%{keyword}%"
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (search_pattern,))
            return [dict(row) for row in cursor.fetchall()]

    def close(self) -> None:
        """
        Закрыть соединение с БД.
        Вызывать при завершении работы с менеджером.
        """
        if self.connection:
            self.connection.close()
            logger.info("Соединение с БД закрыто.")