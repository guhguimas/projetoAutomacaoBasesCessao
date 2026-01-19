import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from app.core.logger import UILogger
from app.controller.robot_controller import RobotController, RobotStatus

class MainWindow:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("CESSÃO PRIME - Automação de Consolidação de Cessões")
        self.root.geometry("900x600")
        self.root.resizable(False, False)

        self._build_layout()
        
        self.logger = UILogger(self.log_area)
        self.logger.log("Sistema iniciado com sucesso", "SUCCESS")

        self.robot = RobotController(
            log_callback=self._safe_log,
            status_callback=self._on_robot_status_change,
            finish_callback=self._on_robot_finish
            )
    
    def _build_layout(self):
        self.button_frame = tk.Frame(self.root)
        self.button_frame.pack(fill=tk.X, padx=10, pady=5)

        self.btn_start = tk.Button(
            self.button_frame,
            text="Start",
            width=12,
            command=self._on_start
        )
        self.btn_start.pack(side=tk.LEFT, padx=5)

        self.btn_stop = tk.Button(
            self.button_frame,
            text="Stop",
            width=12,
            state=tk.DISABLED,
            command=self._on_stop
        )
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        self.log_area = ScrolledText (
            self.root,
            height=30,
            state=tk.DISABLED,
            font=("Consolas", 10)
        )
        self.log_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        

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
    
    def run(self):
        self.root.mainloop()

    def _on_start(self):
        self._clear_logs()
        
        if self.robot and self.robot.status == RobotStatus.RUNNING:
            self.logger.log("Robô já está em execução", "WARNING")

        self.robot.start()
        self.logger.log("Botão START acionado", "INFO")

            
    def _on_stop(self):
        if self.robot:
            self.robot.stop()

        self.logger.log("Botão STOP acionado", "INFO")
    
    def _on_robot_status_change(self, status):
        self.root.after(0, self._update_buttons_state) 

    def _on_robot_finish(self):
        self.root.after(0, self._update_buttons_after_finish) 

    def _safe_log(self, message, level="INFO"):
        self.root.after(0, self.logger.log, message, level)