"""
AutoMatcher: графический интерфейс.
Основные элементы:
- Загрузка файлов.
- Настройка параметров поиска.
- Визуализация результатов в таблице.
- Управление прогрессом.
"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
import threading
from queue import Queue
from data_processor import DataProcessor
from tooltip import ToolTip


class NomenclatureApp:
    def __init__(self, root):
        """Инициализация главного окна приложения."""
        self.root = root
        self.root.title("AutoMatcher")
        self.root.geometry("1200x900") # Фиксированный размер окна

        # Инициализация обработчика данных
        self.processor = DataProcessor()
        self.results = []           # Хранилище результатов
        self.cancel_flag = False    # Флаг отмены обработки
        self.is_processing = False  # Флаг активности процесса

        # Построение интерфейса
        self.create_widgets()
        self.setup_progress_bar()
        self.create_tooltip_system()

    def create_tooltip_system(self):
        """Система контекстных подсказок для заголовков таблицы."""
        self.tooltip_label = ttk.Label(
            self.root,
            text="Нажмите на заголовок для подсказки",
            wraplength=800,
            foreground="gray"
        )
        self.tooltip_label.pack(pady=5)

        # Привязка событий к заголовкам таблицы
        for col in self.tree["columns"]:
            self.tree.heading(col, command=lambda c=col: self.show_tooltip(c))

    def show_tooltip(self, column):
        """Отображает подсказку для выбранной колонки таблицы."""
        descriptions = {
            "Запрос": "Исходный запрос клиента",
            "Номенклатура": "Найденная номенклатура из базы",
            "Код": "Уникальный код товара",
            "Сходство": "Релевантность (0-100, чем выше, тем лучше)",
            "Статус": "Статусы: Оформлено, Товар Производителя, Основной Ассортимент"
        }
        self.tooltip_label.config(text=descriptions.get(column, ""))

    def create_widgets(self):
        """Создает все элементы интерфейса."""

        # Фрейм для загрузки файлов
        file_frame = ttk.LabelFrame(self.root, text="Загрузка файлов")
        file_frame.pack(pady=10, padx=10, fill="x")

        # Кнопки и метки для файлов
        ttk.Button(file_frame, text="1. База номенклатур", command=self.load_nomenclature).grid(row=0, column=0, padx=5)
        self.nomenclature_label = ttk.Label(file_frame, text="Файл не выбран")
        self.nomenclature_label.grid(row=0, column=1, padx=5)

        ttk.Button(file_frame, text="2. Заявка клиента", command=self.load_request).grid(row=1, column=0, padx=5)
        self.request_label = ttk.Label(file_frame, text="Файл не выбран")
        self.request_label.grid(row=1, column=1, padx=5)

        # Фрейм настроек поиска
        settings_frame = ttk.LabelFrame(self.root, text="Настройки поиска")
        settings_frame.pack(pady=10, padx=10, fill="x")

        # Выбор колонок для группировки
        ttk.Label(settings_frame, text="Колонки для объединения:").grid(row=0, column=0, sticky="w")
        self.columns_listbox = tk.Listbox(
            settings_frame,
            selectmode=tk.MULTIPLE, # Множественный выбор
            height=4,
            width=40,
            exportselection=False
        )
        self.columns_listbox.grid(row=0, column=1, padx=5, pady=5)

        # Выбор приоритета
        ttk.Label(settings_frame, text="Приоритет:").grid(row=1, column=0, sticky="w")
        self.priority_var = tk.StringVar(value='Оформлено')
        priority_combobox = ttk.Combobox(
            settings_frame,
            textvariable=self.priority_var,
            values=['Оформлено', 'ТоварПроизводителя', 'ОсновнойАссортимент'],
            state="readonly"    # Запрет ручного ввода
        )
        priority_combobox.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        # Настройка количества результатов
        ttk.Label(settings_frame, text="Кол-во вариантов:").grid(row=2, column=0, sticky="w")
        self.top_n = ttk.Spinbox(settings_frame, from_=1, to=10, width=5)
        self.top_n.set(3) # Значение по умолчанию
        self.top_n.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        # Панель управления
        button_frame = ttk.Frame(self.root)
        button_frame.pack(pady=10)

        ttk.Button(button_frame, text="Обработать заявку", command=self.process_request_async).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Отменить", command=self.cancel_processing).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Сохранить результаты", command=self.save_results).pack(side="left", padx=5)

        # Таблица результатов
        self.tree = ttk.Treeview(
            self.root,
            columns=("Запрос", "Номенклатура", "Код", "Сходство", "Статус"),
            show="headings",
            height=20
        )
        self.tree.pack(pady=10, padx=10, fill="both", expand=True)

        # Настройка колонок таблицы
        i = 1
        for col in ("Запрос", "Номенклатура", "Код", "Сходство", "Статус"):
            self.tree.heading(col, text=col)
            self.tree.column(col,
                             width=int(50 + (545 * i) - (271 * i * i) + ((100/3) * i * i * i)), # Мега-крутая формула вместо того чтобы вводить 5 значений
                             anchor="center")
            i += 1

        # Добавление скроллбара
        scrollbar = ttk.Scrollbar(self.root, orient="vertical", command=self.tree.yview)
        scrollbar.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=scrollbar.set)

    def setup_progress_bar(self):
        """Инициализация прогресс-бара и очереди обновлений."""
        self.progress_queue = Queue()
        self.progress = ttk.Progressbar(
            self.root,
            orient="horizontal",
            length=300,
            mode="determinate"  # Режим с четким прогрессом
        )
        self.progress.pack(pady=10)
        self.check_progress_queue() # Запуск цикла проверки очереди

    def check_progress_queue(self):
        """Обновление прогресс-бара из очереди каждые 100 мс."""
        while not self.progress_queue.empty():
            value = self.progress_queue.get()
            self.progress["value"] = value
            self.root.update_idletasks()
        self.root.after(100, self.check_progress_queue) # Рекурсивный вызов

    def load_nomenclature(self):
        """Загрузка файла номенклатуры через диалоговое окно."""
        path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
        if not path:
            return

        try:
            self.processor.load_nomenclature_data(path)
            self.nomenclature_label.config(text=Path(path).name)
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка загрузки:\n{str(e)}")
            self.nomenclature_label.config(text="Файл не выбран")

    def load_request(self):
        """Загрузка клиентской заявки и заполнение списка колонок."""
        path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
        if not path:
            return

        try:
            self.processor.load_request_data(path)
            self.request_label.config(text=Path(path).name)

            # Обновление списка доступных колонок
            self.columns_listbox.delete(0, tk.END)
            for col in self.processor.request_df.columns:
                self.columns_listbox.insert(tk.END, col)

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка загрузки заявки:\n{str(e)}")
            self.request_label.config(text="Файл не выбран")
            self.columns_listbox.delete(0, tk.END)

    def process_request_async(self):
        """Запуск обработки в отдельном потоке."""
        if self.is_processing:
            messagebox.showwarning("Предупреждение", "Обработка уже запущена")
            return

        # Валидация выбранных колонок
        selected_indices = self.columns_listbox.curselection()
        if not selected_indices:
            messagebox.showerror("Ошибка", "Выберите минимум одну колонку для объединения!")
            return

        # Получение параметров
        selected_columns = [self.columns_listbox.get(i) for i in selected_indices]
        priority = self.priority_var.get()
        top_n = int(self.top_n.get())

        # Сброс состояния
        self.is_processing = True
        self.cancel_flag = False
        self.progress["value"] = 0
        self.results = []

        # Запуск потока
        threading.Thread(
            target=self.run_processing,
            daemon=True,    # Фоновый поток
            args=(selected_columns, priority, top_n)
        ).start()

    def run_processing(self, selected_columns, priority, top_n):
        """Основной цикл обработки данных."""
        try:

            # Привязка callback для обновления прогресса
            self.processor.update_progress = lambda current, total: (
                self.progress_queue.put((current / total) * 100)
            )

            # Вызов метода обработки
            results = self.processor.process_grouped_requests(
                selected_columns=selected_columns,
                priority=priority,
                top_n=top_n
            )

            # Форматирование результатов
            for res in results:
                res['Сходство'] = f"{res['Сходство']:.2f}"

            self.results = results
            self.root.after(0, self.update_results_table)    # Обновление GUI из главного потока
            self.progress_queue.put(100)     # Завершение прогресса

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка обработки: {str(e)}")
        finally:
            # Сброс флагов
            self.is_processing = False
            self.cancel_flag = False
            self.progress_queue.put(0)

    def save_results(self):
        """Сохранение результатов в файл."""
        if not self.results:
            messagebox.showwarning("Предупреждение", "Нет данных для сохранения")
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")]
        )

        if save_path:
            try:
                self.processor.save_results(self.results, save_path)
                messagebox.showinfo("Успех", "Файл успешно сохранен")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")

    def update_results_table(self):
        """Обновление таблицы с результатами."""

        # Очистка предыдущих данных
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Добавление новых записей
        for result in self.results:
            self.tree.insert("", "end", values=(
                result['Запрос'],
                result['Номенклатура'],
                result['Код'],
                result['Сходство'],
                result['Статус']
            ))

    def cancel_processing(self):
        """Остановка текущей обработки."""
        self.cancel_flag = True
        self.progress_queue.put(0)  # Сброс прогресса
        self.is_processing = False