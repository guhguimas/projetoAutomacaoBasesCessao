import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from app.core.logger import UILogger
from app.controller.robot_controller import RobotController, RobotStatus
from app.core.file_manager import FileManager
from config.ui_config import FILE_ROWS
from tkinter import ttk

class MainWindow:
    def __init__(self):
        self._layout_built = False
        self.root = tk.Tk()
        self.root.title("CESSÃO PRIME - Automação de Consolidação de Cessões")
        self.root.geometry("900x600")
        self.root.resizable(False, False)

        self.progress_var = tk.IntVar(value=0)
        self.progress_text_var = tk.StringVar(value="Aguardando início...")

        self._reset_progress()
        self.status_labels = {}
        self._build_layout()

        self.logger = UILogger(self.log_area)
        self.logger.log("Sistema iniciado com sucesso", "SUCCESS")

        self.file_manager = FileManager()

        self.robot = RobotController(
            log_callback=self._safe_log,
            status_callback=self._on_robot_status_change,
            finish_callback=self._on_robot_finish,
            progress_callback=self._safe_progress
            )
    
    def _build_layout(self):
        if self._layout_built:
            return
        
        self.button_frame = tk.Frame(self.root)
        self.button_frame.pack(fill=tk.X, padx=10, pady=5)

        self.btn_start = tk.Button(
            self.button_frame,
            text="INICIAR",
            width=12,
            command=self._on_start
        )
        self.btn_start.pack(side=tk.LEFT, padx=5)

        self.btn_stop = tk.Button(
            self.button_frame,
            text="PARAR",
            width=12,
            state=tk.DISABLED,
            command=self._on_stop
        )
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        self.files_frame = tk.LabelFrame(self.root, text="Seleção de Arquivos", padx=10, pady=10)
        self.files_frame.pack(fill=tk.X, padx=10, pady=5)

        row = tk.Frame(self.files_frame)
        row.pack(fill=tk.X, pady=2)

        # lbl_name = tk.Label(row, text="Planilha Base (Cessão)", width=28, anchor="w")
        # lbl_name.pack(side=tk.LEFT)

        # self.lbl_status_cessao = tk.Label(row, text="Não selecionado", width=18, anchor="w")
        # self.lbl_status_cessao.pack(side=tk.LEFT, padx=5)
        # self.status_labels["cessao"] = self.lbl_status_cessao

        # btn_select = tk.Button(row, text="Selecionar", width=12, command=lambda: self._select_file("cessao"))
        # btn_select.pack(side=tk.RIGHT)
        
        for key, text in FILE_ROWS:
            self._add_file_row(key, text)

        self.progress_frame = tk.Frame(self.root)
        self.progress_frame.pack(fill=tk.X, padx=10, pady=5)

        self.progress_label = tk.Label(self.progress_frame, textvariable=self.progress_text_var, anchor="w")
        self.progress_label.pack(fill=tk.X)

        self.progress_bar = ttk.Progressbar(
            self.progress_frame,
            variable = self.progress_var,
            maximum = 100,
            mode = "determinate"
        )
        self.progress_bar.pack(fill=tk.X, pady=4)

        self.log_area = ScrolledText (
            self.root,
            height=30,
            state=tk.DISABLED,
            font=("Consolas", 10)
        )
        self.log_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.layout_built = True

    def _update_buttons_state(self):
        if self.robot.status == RobotStatus.RUNNING:
            self.btn_start.config(state=tk.DISABLED)
            self.btn_stop.config(state=tk.NORMAL)
        else:
            self.btn_start.config(state=tk.NORMAL)
            self.btn_stop.config(state=tk.DISABLED)
    
    def _update_buttons_after_finish(self):
        self.btn_start.config(state=tk.NORMAL)
        self.btn_stop.config(state=tk.DISABLED)

    def _clear_logs(self):
        self.log_area.config(state=tk.NORMAL)
        self.log_area.delete("1.0", tk.END)
        self.log_area.config(state=tk.DISABLED)

    def _reset_ui(self):
        self.file_manager.reset()
        self._reset_progress()

        for key, label in self.status_labels.items():
            label.config(text="Não selecionado")

        self._clear_logs()
        self.logger.log("Programa resetado.", "INFO")

    def run(self):
        self.root.mainloop()

    def _on_start(self):
        self._reset_progress()

        if self.robot and self.robot.status == RobotStatus.RUNNING:
            self.logger.log("Robô já está em execução", "WARNING")
            return
        
        missing = self.file_manager.get_missing_files()
        snapshot = self.file_manager.snapshot()

        if missing:
            msg = "Faltam arquivos:\n\n" + "\n".join(missing) + "\n\nDeseja selecionar agora?"
            select_row = messagebox.askyesno("Arquivos ausentes", msg)
            if select_row:
                for key in missing:
                    self._select_file(key)

        if missing:
            msg2 = "Ainda faltam arquivos:\n\n" + "\n".join(missing) + "\n\nDeseja continuar mesmo assim (execução parcial)?"
            ok = messagebox.askyesno("Execução parcial", msg2)
            if not ok:
                self.logger.log("Execução cancelada: arquivos ausentes.", "WARNING")
                self.file_manager.restore(snapshot)
                self._refresh_file_status_labels()
                self._reset_ui()                
                return

        self.robot.start()
        self.logger.log("Botão INICIAR acionado", "INFO")

        self._clear_logs()
            
    def _on_stop(self):
        if self.robot:
            self.robot.stop()
        self.root.after(0, self._reset_ui)
        self.logger.log("Botão PARAR acionado", "INFO")

        self._reset_ui()
    
    def _on_robot_status_change(self, status):
        self.root.after(0, self._update_buttons_state) 

    def _on_robot_finish(self):
        self.root.after(0, self._reset_progress)
        self.root.after(0, self._reset_ui)

    def _safe_log(self, message, level="INFO"):
        self.root.after(0, self.logger.log, message, level)

    def _select_file(self, key):
        path = filedialog.askopenfilename(
            title = "Selecionar o Arquivo",
            filetypes=[("planilhas", "*.xlsx *.csv"), ("Todos os arquivos", "*.*")]
        )
        if not path:
            return

        self.file_manager.set_file(key, path)

        if key in self.status_labels:
            self.status_labels[key].config(text="Selecionado")

    def _refresh_file_status_labels(self):
        for key, label in self.status_labels.items():
            if self.file_manager.files.get(key):
                label.config(text="Selecionado")
            else:
                label.config(text="Não Selecionado")

    def _reset_progress(self):
        self.progress_var.set(0)
        self.progress_text_var.set("Aguardando início...")

    def _update_progress(self, current_step, total_steps, message):
        percent = int((current_step / total_steps) * 100)
        self.progress_var.set(percent)
        self.progress_text_var.set(f"Etapa {current_step} de {total_steps} - {message}")

    def _add_file_row(self, key, label_text):
        if key in self.status_labels:
            return

        row = tk.Frame(self.files_frame)
        row.pack(fill=tk.X, pady=2)

        lbl_name = tk.Label(row, text=label_text, width=28, anchor="w")
        lbl_name.pack(side=tk.LEFT)

        lbl_status = tk.Label(row, text="Não selecionado", width=18, anchor="w")
        lbl_status.pack(side=tk.LEFT, padx=5)

        self.status_labels[key] = lbl_status

        btn_select = tk.Button(
            row,
            text="Selecionar",
            width=12,
            command=lambda k=key: self._select_file(k)
        )
        btn_select.pack(side=tk.RIGHT)

    def _safe_progress(self, current_step, total_steps, message):
        self.root.after(0, self._update_progress, current_step, total_steps, message)