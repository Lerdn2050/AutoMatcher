"""
Точка входа в приложение AutoMatcher.
Запускает графический интерфейс.
"""
import tkinter as tk
from gui import NomenclatureApp

if __name__ == "__main__":
    root = tk.Tk()                  # Создание корневого окна Tkinter
    app = NomenclatureApp(root)     # Инициализация главного класса приложения
    root.mainloop()                 # Запуск основного цикла обработки событий