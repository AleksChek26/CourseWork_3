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
        """Получить вакансии работодателя"""
        url = f"{self.base_url}/vacancies"
        params = {'employer_id': employer_id, 'per_page': 100}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        return []

    def search_employers(self, query: str) -> List[Dict]:
        """Поиск работодателей по названию"""
        url = f"{self.base_url}/employers"
        params = {'text': query, 'per_page': 10}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        return []
