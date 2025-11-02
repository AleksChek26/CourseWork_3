from api_client import HHApiClient
from db_manager import DBManager
from config import HH_API_URL
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Список ID работодателей (пример)
EMPLOYER_IDS = [
        1455,  # Яндекс
        78638,  # Тинькофф
        15478,  # VK
        227780, # Роснефть
        39305, # Газпром нефть
        3529,  # Сбер
        80,  # Альфа-Банк
        2748, # Ростелеком
        84585,  # Авито
        2180,  # МТС
    ]

def load_data_to_db(db_manager: DBManager, api_client: HHApiClient, employer_ids: list[int]):
    """Загрузить данные о компаниях и вакансиях в БД"""
    logger.info("Начинаем загрузку данных в БД...")

    for emp_id in employer_ids:
        # Получаем данные о работодателе
        employer = api_client.get_employer(emp_id)
        if not employer:
            logger.warning(f"Не удалось получить данные для работодателя {emp_id}")
            continue

        # Добавляем работодателя в БД
        db_manager.add_employer(employer)
        logger.info(f"Добавлена компания: {employer['name']}")

        # Получаем и добавляем вакансии
        vacancies = api_client.get_vacancies_by_employer(emp_id)
        for vacancy in vacancies:
            db_manager.add_vacancy(vacancy, emp_id)
        logger.info(f"Добавлено вакансий для {employer['name']}: {len(vacancies)}")

    logger.info("Загрузка данных завершена.")


def show_menu(db_manager: DBManager):
    """Показать интерактивное меню для пользователя"""
    while True:
        print("\n=== Меню работы с базой вакансий ===")
        print("1. Список компаний и количество вакансий")
        print("2. Все вакансии (с деталями)")
        print("3. Средняя зарплата по всем вакансиям")
        print("4. Вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("6. Выход")

        choice = input("\nВыберите действие (1-6): ").strip()

        if choice == '1':
            companies = db_manager.get_companies_and_vacancies_count()
            print("\nСписок компаний и количество вакансий:")
            for name, count in companies:
                print(f"{name}: {count} вакансий")

        elif choice == '2':
            vacancies = db_manager.get_all_vacancies()
            print(f"\nВсего вакансий: {len(vacancies)}")
            for v in vacancies:
                print(f"- {v['company_name']} | {v['vacancy_title']} | "
                      f"{v['salary_from']}-{v['salary_to']} {v['currency']} | {v['url']}")


        elif choice == '3':
            avg_salary = db_manager.get_avg_salary()
            print(f"\nСредняя зарплата: {avg_salary:.0f} руб.")


        elif choice == '4':
            high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
            print(f"\nВакансий с зарплатой выше средней: {len(high_salary_vacancies)}")
            for v in high_salary_vacancies:
                print(f"- {v['company_name']} | {v['vacancy_title']} | "
                      f"{v['salary_from']}-{v['salary_to']} {v['currency']} | {v['url']}")


        elif choice == '5':
            keyword = input("Введите ключевое слово для поиска: ").strip()
            if keyword:
                results = db_manager.get_vacancies_with_keyword(keyword)
                print(f"\nНайдено вакансий по запросу '{keyword}': {len(results)}")
                for v in results:
                    print(f"- {v['company_name']} | {v['vacancy_title']} | "
                          f"{v['salary_from']}-{v['salary_to']} {v['currency']} | {v['url']}")
            else:
                print("Введите непустое ключевое слово.")

        elif choice == '6':
            print("До свидания!")
            break

        else:
            print("Неверный выбор. Попробуйте ещё раз.")


def main():
    api_client = HHApiClient(HH_API_URL)
    db_manager = DBManager()

    try:
        # 1. Создать БД, если её нет
        db_manager.create_database()

        # 2. Переподключиться к целевой БД
        db_manager.reconnect_to_target_db()

        # 3. Создать таблицы, если их нет
        db_manager.create_tables()

        # 4. Загрузить данные
        load_data_to_db(db_manager, api_client, EMPLOYER_IDS)

        # 5. Показать меню
        show_menu(db_manager)

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
