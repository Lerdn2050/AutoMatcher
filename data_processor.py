"""
AutoMatcher: модуль обработки данных.
Основные функции:
- Конвертация Excel в SQLite.
- Поиск совпадений через алгоритм BM25.
- Фильтрация и ранжирование товаров.
"""
import pandas as pd
import sqlite3
import os
from pathlib import Path
from rank_bm25 import BM25Okapi


class DataProcessor:
    def __init__(self):
        """Инициализация обработчика данных."""
        self.nomenclature_df = None
        self.request_df = None
        self.replacements = {
            'перманентный': 'перм', 'маркер': 'марк', 'мультифора': 'файл',
            'грузоподъемностью': 'груз', 'пластиковый': 'пласт', 'металлический': 'мет',
            'тн': 'тонн', 'корректирующее': 'корр', 'самоклеящаяся': 'самокл',
            'гибкая': 'гибк', 'регистратор': 'регистр', 'кальцинированная': 'кальц',
            'гост': 'стандарт', 'ассорти': 'разные цвета', 'арочный': 'дуга'
        }

    def convert_excel_to_sqlite(self, excel_path, db_path, table_name):
        """Конвертирует Excel-файл в SQLite-базу.
                Args:
                    excel_path (str): Путь к Excel-файлу.
                    db_path (str): Путь для сохранения SQLite-базы.
                    table_name (str): Название таблицы.
                """
        try:
            df = pd.read_excel(excel_path, engine='openpyxl')
            conn = sqlite3.connect(db_path)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
        except Exception as e:
            raise Exception(f"Ошибка конвертации: {str(e)}")

    def load_nomenclature_data(self, path):
        """Загружает данные номенклатуры из Excel или SQLite.
                Args:
                    path (str): Путь к Excel-файлу.
                """

        # Если базы нет, конвертируем Excel в SQLite
        db_path = Path(path).with_suffix('.db')
        if not os.path.exists(db_path):
            self.convert_excel_to_sqlite(path, db_path, 'nomenclature')

        # Загрузка данных из SQLite
        conn = sqlite3.connect(db_path)
        self.nomenclature_df = pd.read_sql('SELECT * FROM nomenclature', conn)
        conn.close()

        # Добавление недостающих колонок
        required_cols = ['Номенклатура', 'Код', 'Оформлено',
                         'ТоварПроизводителя', 'ОсновнойАссортимент']
        for col in required_cols:
            if col not in self.nomenclature_df.columns:
                self.nomenclature_df[col] = 'Нет' # Значение по умолчанию

    def save_results(self, results, path):
        """Сохраняет результаты поиска в файл (Excel/CSV).
            Args:
                results (list): Список словарей с результатами.
                path (str): Путь для сохранения файла.
            """
        df = pd.DataFrame(results)

        # Конвертация строковых значений 'Сходство' в числовой формат
        if 'Сходство' in df.columns:
            df['Сходство'] = df['Сходство'].astype(float)

        # Выбор формата экспорта с учетом локализации для CSV
        if path.endswith('.xlsx'):
            df.to_excel(path, index=False)
        elif path.endswith('.csv'):
            # Использование ';' как разделителя и ',' для десятичных
            df.to_csv(path, index=False, sep=';', decimal=',')
        else:
            raise ValueError("Неподдерживаемый формат файла")

    def update_progress(self, current, total):
        pass

    def process_grouped_requests(self, selected_columns, priority, top_n):
        """Обрабатывает группированные запросы.
                Args:
                    selected_columns (list): Колонки для группировки.
                    priority (str): Приоритет сортировки (например, 'Оформлено').
                    top_n (int): Количество возвращаемых вариантов.
                Returns:
                    list: Результаты поиска.
                """
        # Проверка загрузки данных
        if self.request_df is None or self.nomenclature_df is None:
            raise ValueError("Данные не загружены")

        # Группировка запросов по выбранным колонкам
        self.request_df['combined_key'] = self.request_df[selected_columns].astype(str).agg(' | '.join, axis=1)
        grouped = self.request_df.groupby('combined_key')

        # Подготовка данных для BM25
        self.nomenclature_df['processed'] = self.nomenclature_df['Номенклатура'].apply(self.preprocess_text)
        tokenized_names = [text.split() for text in self.nomenclature_df['processed']]
        bm25 = BM25Okapi(tokenized_names)  # Инициализация модели BM25

        results = []
        total_groups = len(grouped)

        # Обработка каждой группы запросов
        for group_idx, (group_name, group_df) in enumerate(grouped):
            group_results = []

            # Сбор частей запроса из группы
            query_parts = []
            for _, row in group_df.iterrows():
                parts = [str(row[col]) for col in selected_columns if pd.notna(row[col])]
                query_parts.extend(parts)

            # Формирование уникального запроса
            unique_query = ' '.join(sorted(set(query_parts), key=query_parts.index))
            processed_query = self.preprocess_text(unique_query).split()

            # Расчет релевантности через BM25
            scores = bm25.get_scores(processed_query)
            min_score, max_score = min(scores), max(scores)

            # Нормализация оценок в диапазон 0-100
            if max_score == min_score:
                normalized = [100] * len(scores) if max_score > 0 else [0] * len(scores)
            else:
                normalized = [(s - min_score) / (max_score - min_score) * 100 for s in scores]

            # Бонус за совпадение начала
            for idx, name in enumerate(self.nomenclature_df['processed']):
                if name.startswith(' '.join(processed_query)):
                    normalized[idx] = min(normalized[idx] + 5, 100)

            # Сортировка и фильтрация результатов
            temp_df = self.nomenclature_df.copy()
            temp_df['similarity'] = normalized
            sorted_df = temp_df.sort_values(
                [priority, 'similarity'],
                ascending=[False, False]
            ).head(top_n * 2)  # Взято в 2 раза больше для отсева дубликатов

            # Фильтрация
            unique_codes = set()
            for _, row in sorted_df.iterrows():
                if row['Код'] not in unique_codes:
                    group_results.append({
                        'Запрос': group_name,
                        'Номенклатура': row['Номенклатура'],
                        'Код': row['Код'],
                        'Сходство': row['similarity'],
                        'Статус': self.get_status(row)
                    })
                    unique_codes.add(row['Код'])
                    if len(unique_codes) >= top_n:
                        break

            results.extend(group_results)
            self.update_progress(group_idx + 1, total_groups)

        return results

    def load_request_data(self, path):
        """Загружает данные клиентской заявки из Excel.
            Особенности:
            - Автоматически определяет строку с заголовками
            - Игнорирует технические колонки ('Unnamed')
            - Заполняет пропущенные значения предыдущими
            """

        # Поиск заголовка в первых 10 строках
        temp_df = pd.read_excel(path, header=None, nrows=10, engine='openpyxl')
        header_row = self.find_header_row(temp_df)

        # Чтение данных с фильтрацией ненужных колонок
        self.request_df = pd.read_excel(
            path,
            header=header_row,
            engine='openpyxl',
            usecols=lambda x: 'Unnamed' not in str(x)   # Игнорирование служебных колонок
        ).dropna(how='all').ffill()        # Удаление пустых строк и заполнение пропусков

    def find_header_row(self, df):
        """Ищет строку с заголовками таблицы по наличию слова 'товар'.
            Возвращает:
                int: Номер строки с заголовком (0 если не найден).
            """
        for idx, row in df.iterrows():
            if any('товар' in str(cell).lower() for cell in row):
                return idx
        return 0  # Возврат первой строки как заголовка по умолчанию

    def preprocess_text(self, text):
        """Предобработка текста для поиска:
            1. Приведение к нижнему регистру
            2. Удаление спецсимволов (остаются буквы, цифры и пробелы)
            3. Замена слов по словарю сокращений
            """
        text = str(text).lower()
        cleaned = [char for char in text if char.isalnum() or char.isspace()]
        text = ''.join(cleaned)

        for k, v in self.replacements.items(): # Применение замен из словаря
            text = text.replace(k, v)
        return text

    def process_data(self, column_name, priority_var, top_n, progress_callback=None):
        """Основной метод обработки данных. Этапы:
            1. Препроцессинг названий товаров
            2. Построение модели BM25
            3. Итеративная обработка каждого запроса
            4. Нормализация оценок и добавление бонусов
            5. Фильтрация и сохранение результатов
            """

        # Токенизация названий для BM25
        self.nomenclature_df['processed'] = self.nomenclature_df['Номенклатура'].apply(self.preprocess_text)
        tokenized_names = [text.split() for text in self.nomenclature_df['processed']]
        bm25 = BM25Okapi(tokenized_names)

        results = []
        total_queries = len(self.request_df[column_name])
        processed_count = 0
        for idx, query in enumerate(self.request_df[self.column_var.get()]):
            if self.cancel_flag:
                break  # Прерывание при отмене

            processed_query = self.preprocess_text(query).split()
            scores = bm25.get_scores(processed_query)

            # Нормализация оценок в диапазон 0-100
            min_score, max_score = min(scores), max(scores)
            if max_score == min_score:
                normalized = [100] * len(scores) if max_score > 0 else [0] * len(scores)
            else:
                normalized = [(s - min_score) / (max_score - min_score) * 100 for s in scores]

            # Бонус за совпадение начала строки
            for idx_name, name in enumerate(self.nomenclature_df['processed']):
                if name.startswith(' '.join(processed_query)):
                    normalized[idx_name] = min(normalized[idx_name] + 5, 100)

            # Сортировка и выбор топ-N результатов
            results = self.nomenclature_df.copy()
            results['similarity'] = normalized
            results = results.sort_values(
                [priority_var, 'similarity'],
                ascending=[False, False]
            ).head(int(top_n))

            # Формирование итоговой структуры
            for _, row in results.iterrows():
                self.results.append({
                    'Запрос': query,
                    'Номенклатура': row['Номенклатура'],
                    'Код': row['Код'],
                    'Сходство': f"{row['similarity']:.2f}",
                    'Статус': self.get_status(row)
                })
            # Дополнительное обновление прогресса каждые 10 запросов
            processed_count += 1
            if processed_count % 10 == 0 or processed_count == total_queries:
                progress = processed_count / total_queries * 100
                if progress_callback:
                    progress_callback(progress)
        return results

    def get_status(self, row):
        """Формирует строку статусов товара на основе флагов.
            Возможные статусы:
            - Оформлено
            - Товар Производителя
            - Основной Ассортимент
            """
        status = []
        if row['Оформлено'] == 'Да': status.append("Оформлено")
        if row['ТоварПроизводителя'] == 'Да': status.append("Товар Производителя")
        if row['ОсновнойАссортимент'] == 'Да': status.append("Основной Ассортимент")
        return ", ".join(status) if status else "—"