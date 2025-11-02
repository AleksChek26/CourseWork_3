import requests
from typing import List, Dict, Optional


class HHApiClient:
    """Клиент для взаимодействия с API hh.ru"""

    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_employer(self, employer_id: int) -> Optional[Dict]:
        """Получить данные о работодателе"""
        url = f"{self.base_url}/employers/{employer_id}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return None

    def get_vacancies_by_employer(self, employer_id: int) -> List[Dict]:
        """Получить все вакансии работодателя (с пагинацией)"""
        url = f"{self.base_url}/vacancies"
        params = {
            'employer_id': employer_id,
            'per_page': 100,
            'page': 0
        }
        all_vacancies = []

        while True:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                break

            data = response.json()
            vacancies = data.get('items', [])
            if not vacancies:
                break

            all_vacancies.extend(vacancies)
            params['page'] += 1

            # Защита от бесконечного цикла
            if params['page'] >= 20:  # максимум 2000 вакансий
                break

        return all_vacancies
