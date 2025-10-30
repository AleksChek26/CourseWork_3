from api_client import HHApiClient
from db_manager import DBManager
from config import HH_API_URL
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        print("3. Средняя зарплата по вакансиям")
        print("4. Вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("6. Выход")

        choice = input("\nВыберите действие (1-6): ").strip()

        if choice == '1':
            companies = db_manager.get_companies_and_vacancies_count()
            print("\nКомпании и количество вакансий:")
            for name, count in companies:
                print(f"- {name}: {count} вакансий")

        elif choice == '2':
            vacancies = db_manager.get_all_vacancies()
            print("\nВсе вакансии:")
            for vac in vacancies:
                salary_info = f"{vac['salary_from']}-{vac['salary_to']} {vac['currency']}" if vac[
                    'salary_from'] else "не указана"
                print(f"- {vac['company_name']} | {vac['vacancy_title']} | Зарплата: {salary_info} | {vac['url']}")

        elif choice == '3':
            avg_salary = db_manager.get_avg_salary()
            print(f"\nСредняя зарплата по вакансиям: {avg_salary:.0f} руб.")

        elif choice == '4':
            high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
            print("\nВакансии с зарплатой выше средней:")
            for vac in high_salary_vacancies:
                salary_info = f"{vac['salary_from']}-{vac['salary_to']} {vac['currency']}"
                print(f"- {vac['company_name']} | {vac['vacancy_title']} | {salary_info} | {vac['url']}")

        elif choice == '5':
            keyword = input("Введите ключевое слово для поиска: ").strip()
            if keyword:
                keyword_vacancies = db_manager.get_vacancies_with_keyword(keyword)
                print(f"\nВакансии с ключевым словом '{keyword}':")
                if keyword_vacancies:
                    for vac in keyword_vacancies:
                        salary_info = f"{vac['salary_from']}-{vac['salary_to']} {vac['currency']}" if vac[
                            'salary_from'] else "не указана"
                        print(f"- {vac['company_name']} | {vac['vacancy_title']} | {salary_info} | {vac['url']}")
                else:
                    print("Вакансий не найдено.")
            else:
                print("Ключевое слово не введено.")

        elif choice == '6':
            print("До свидания!")
            break

        else:
            print("Неверный выбор. Пожалуйста, введите число от 1 до 6.")


def main():
    # Инициализация компонентов
    api_client = HHApiClient(HH_API_URL)
    db_manager = DBManager()

    # Создание таблиц в БД
    db_manager.create_tables()

    # Список ID работодателей (пример)
    EMPLOYER_IDS = [
        1455,  # Яндекс
        78638,  # Тинькофф
        15478,  # VK
        4192670,  # Skyeng
        370481,  # Лаборатория Касперского
        24109,  # Сбербанк
        921364,  # Альфа-Банк
        1122466,  # Ситимобил
        641729,  # Авито
        2180,  # МТС
    ]

    # Загрузка данных в БД (можно отключить, если данные уже загружены)
    load_data_to_db(db_manager, api_client, EMPLOYER_IDS)

    # Запуск интерактивного меню
    show_menu(db_manager)


if __name__ == "__main__":
    main()