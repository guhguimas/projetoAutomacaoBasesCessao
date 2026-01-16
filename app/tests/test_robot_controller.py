import time
from app.core.robot_controller import RobotController


def log_to_console(message):
    print(message)


robot = RobotController(log_callback=log_to_console)

print(">>> START ROBÔ")
robot.start()

# espera até a etapa 2 ou 3
time.sleep(2.5)

print(">>> STOP ROBÔ")
robot.stop()

# espera um pouco pra ver se algo ainda roda
time.sleep(2)

print(">>> TESTE FINALIZADO")