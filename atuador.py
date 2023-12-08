import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet

broker_address = "localhost"
port = 1883
topic = "atuador/status"

# Chave Fernet (deve ser a mesma chave usada para criptografar as mensagens)
chave_criptografia = '_t5Xz_HncrR186Vf-h4pFdjnlyhKWcA1gDo1ckeDP3g='.encode()  # Substitua com sua chave
cipher = Fernet(chave_criptografia)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao servidor MQTT")
        client.subscribe(topic)
    else:
        print(f"Falha na conexão ao servidor MQTT com código de retorno {rc}")


def on_message(client, userdata, msg):
    mensagem_criptografada = msg.payload
    valor_descriptografado = cipher.decrypt(mensagem_criptografada).decode()
    #print(valor_descriptografado)
    print("------")

    print("Mensagem recebida:")
    print(f"Tópico: {msg.topic}")
    print(f"Mensagem: {valor_descriptografado}")

    if valor_descriptografado == "1":
        print("Alarme ativado, Email enviado")
    else:
        print("Alarme desativado")


if __name__ == "__main__":
    print("Iniciando atuador")
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker_address, port, 60)
    print("Conectado ao servidor MQTT")

    client.loop_forever()
