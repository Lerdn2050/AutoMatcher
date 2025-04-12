"""
Модуль для всплывающих подсказок в интерфейсе.
Реализация: ToolTip с привязкой к виджетам Tkinter.
"""
import tkinter as tk

class ToolTip:
    def __init__(self, widget, text):
        """Инициализация подсказки для виджета.
                Args:
                    widget: Целевой виджет Tkinter
                    text (str): Текст подсказки
                """
        self.widget = widget
        self.text = text
        self.tip_window = None  # Окно подсказки (создается при наведении)

        # Привязка обработчиков событий
        self.widget.bind("<Enter>", self.show_tip)  # При наведении курсора
        self.widget.bind("<Leave>", self.hide_tip)  # При уходе курсора

    def show_tip(self, event=None):
        """Показывает подсказку рядом с виджетом."""
        if self.tip_window:
            return

        # Позиционирование окна (+25px от виджета)
        x = self.widget.winfo_rootx() + 25
        y = self.widget.winfo_rooty() + 25

        # Создание окна без рамки и заголовка
        self.tip_window = tk.Toplevel(self.widget)
        self.tip_window.wm_overrideredirect(True)   # Удаление системной рамки
        self.tip_window.wm_geometry(f"+{x}+{y}")    # Установка позиции

        # Настройка стиля подсказки
        label = tk.Label(
            self.tip_window,
            text=self.text,
            background="#ffffe0",   # Цвет фона (светло-желтый)
            relief="solid",         # Граница
            borderwidth=1           # Толщина границы
        )
        label.pack()

    def hide_tip(self, event=None):
        """Скрывает подсказку."""
        if self.tip_window:
            self.tip_window.destroy()   # Уничтожение окна
        self.tip_window = None