import tkinter as tk
from tkinter import messagebox
import threading
import os
import signal
import time
import paho.mqtt.client as mqtt

def start_sensor():
    os.system("python sensor.py")

def start_controlador1():
    os.system("python controlador.py")

def start_controlador2():
    os.system("python controlador2.py")

def start_atuador():
    os.system("python atuador.py")

class MainApplication:
    def __init__(self, master):
        self.master = master
        master.title("Controle do Sistema")

        self.sensor_running = False

        self.label = tk.Label(master, text="Controle do Sensor")
        self.label.pack()

        self.sensor_button = tk.Button(master, text="Ligar Sensor", command=self.toggle_sensor)
        self.sensor_button.pack()

        self.controladores_button = tk.Button(master, text="Iniciar Controladores", command=self.start_controladores)
        self.controladores_button.pack()

        self.atuador_button = tk.Button(master, text="Iniciar Atuador", command=start_atuador)
        self.atuador_button.pack()

    def toggle_sensor(self):
        self.sensor_running = not self.sensor_running
        status = "Ligado" if self.sensor_running else "Desligado"
        messagebox.showinfo("Status do Sensor", f"Sensor {status}")

    def start_controladores(self):
        if not self.sensor_running:
            messagebox.showwarning("Aviso", "O sensor está desligado. Ligue o sensor antes de iniciar os controladores.")
            return

        # Iniciar threads para os controladores e o sensor
        sensor_thread = threading.Thread(target=start_sensor)
        controlador_thread = threading.Thread(target=start_controlador1)
        controladors_thread = threading.Thread(target=start_controlador2)
        atuador_thread = threading.Thread(target=start_atuador)

        # Iniciar as threads
        sensor_thread.start()
        time.sleep(2)  # Aguardar um pouco para garantir que o sensor seja iniciado antes dos controladores
        controlador_thread.start()
        controladors_thread.start()
        atuador_thread.start()

        # Aguardar até que todas as threads terminem
        sensor_thread.join()
        controlador_thread.join()
        controladors_thread.join()
        atuador_thread.join()

if __name__ == "__main__":
    root = tk.Tk()
    app = MainApplication(root)
    root.mainloop()
