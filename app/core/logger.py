import datetime
import tkinter as tk
from tkinter.scrolledtext import ScrolledText

class UILogger:
    def __init__(self, text_widget: ScrolledText):
        self.text_widget = text_widget

    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] [{level}] {message}\n"

        self.text_widget.configure(state=tk.NORMAL)
        self.text_widget.insert(tk.END, formatted_message)
        self.text_widget.configure(state=tk.DISABLED)
        self.text_widget.see(tk.END)
        