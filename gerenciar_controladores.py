import subprocess
from random import choice
import time
import os

dados_recebidos = 0  # Inicializa o contador de dados recebidos
processo_ativo = None  # Armazena o objeto Popen do controlador ativo

def iniciar_controlador1():
    global dados_recebidos, processo_ativo
    print("Iniciando Controlador 1")
    # Obtém o caminho absoluto para controlador.py
    script_path = os.path.abspath("controlador.py")
    processo_ativo = subprocess.Popen(["python3", script_path])
    dados_recebidos += 1  # Incrementa o contador após a execução do controlador

def iniciar_controlador2():
    global dados_recebidos, processo_ativo
    print("Iniciando Controlador 2")
    # Obtém o caminho absoluto para controlador2.py
    script_path = os.path.abspath("controlador2.py")
    processo_ativo = subprocess.Popen(["python3", script_path])
    dados_recebidos += 1  # Incrementa o contador após a execução do controlador

def finalizar_controlador():
    global processo_ativo
    if processo_ativo:
        processo_ativo.terminate()
        processo_ativo = None

def gerenciar_controladores():
    global dados_recebidos

    while True:
        # Verifica se o número de dados recebidos é um múltiplo de 3
        if dados_recebidos % 3 == 0 and dados_recebidos > 0:
            # Finaliza o controlador ativo antes de trocar
            finalizar_controlador()

            # Troca para o próximo controlador
            if choice([True, False]):  # Escolhe aleatoriamente entre True e False
                print("Trocando para Controlador 2")
                finalizar_controlador()
                iniciar_controlador2()
            else:
                print("Trocando para Controlador 1")
                finalizar_controlador()
                iniciar_controlador1()
        else:
            # Se o número de dados recebidos não for um múltiplo de 3, continua com o controlador ativo
            controladores = [iniciar_controlador1, iniciar_controlador2]
            controlador_ativo = choice(controladores)
            print(f"Iniciando {controlador_ativo.__name__}")
            controlador_ativo()

            # Aguarda um tempo antes de verificar novamente
            time.sleep(5)  # Ajuste o tempo conforme necessário

if __name__ == "__main__":
    gerenciar_controladores()
