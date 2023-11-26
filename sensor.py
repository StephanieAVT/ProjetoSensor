import random
import time
import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
import cipher as cipher


# Configurações do servidor MQTT
broker_address = "localhost"  # Endereço do servidor MQTT
port = 1883  # Porta padrão do MQTT
topic = "sensor/movimento"  # Tópico para enviar os valores do sensor

# Gera uma chave Fernet aleatória
chave_criptografia = Fernet.generate_key()

# Cria um objeto Fernet usando a chave gerada
cipher = Fernet(chave_criptografia)

# Exibe a chave gerada (opcional)
print("Chave de Criptografia Gerada:", chave_criptografia)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao servidor MQTT")
        # Inscreve-se no tópico MQTT ao se conectar
        client.subscribe(topic)
    else:
        print(f"Falha na conexão ao servidor MQTT com código de retorno {rc}")

def on_message(client, userdata, msg):
    # Descriptografa a mensagem recebida
    mensagem_descriptografada = cipher.decrypt(msg.payload).decode()
    print(f"Mensagem recebida: {mensagem_descriptografada}")

def sensor_movimento(client):
    while True:
        try:
            # Gera um valor aleatório de 0 ou 1 para simular o sensor de movimento
            valor = str(random.randint(0, 1))

            # Criptografa o valor antes de publicar no tópico MQTT
            valor_criptografado = cipher.encrypt(str(valor).encode())
            client.publish(topic, valor_criptografado)
            print(f"Mensagem Criptografada: {valor_criptografado} /n")


            # Verifica se o valor é 1 (movimento detectado)
            if valor == 1:
                print("Movimento detectado!")
            else:
                print("Nenhum movimento detectado.")

            # Aguarda 5 segundos antes de gerar o próximo valor
            time.sleep(5)

        except KeyboardInterrupt:
            # Encerra a execução do programa quando o usuário pressiona Ctrl + C
            print("Programa encerrado pelo usuário")
            break

if __name__ == "__main__":
    # Configura o cliente MQTT
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message  # Adiciona a função de callback para mensagens

    # Conecta ao servidor MQTT
    client.connect(broker_address, port, 60)

    # Inicia um thread para processar a comunicação MQTT em segundo plano
    client.loop_start()

    try:
        # Inicia o sensor de movimento
        sensor_movimento(client)
    except KeyboardInterrupt:
        # Encerra a execução do programa quando o usuário pressiona Ctrl + C
        print("Programa encerrado pelo usuário")

    # Desconecta do servidor MQTT ao finalizar o programa
    client.disconnect()
