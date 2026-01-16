import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from app.core.logger import UILogger

class MainWindow:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("CESSÃO PRIME - Automação de Consolidação de Cessões")
        self.root.geometry("900x600")
        self.root.resizable(False, False)

        self._build_layout()

    def _build_layout(self):
        self.log_area = ScrolledText (
            self.root,
            height=30,
            state=tk.DISABLED,
            font=("Consolas", 10)
        )
        self.log_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.logger = UILogger(self.log_area)
        self.logger.log("Sistema iniciado com sucesso", "SUCCESS")

    def run(self):
        self.root.mainloop()
        