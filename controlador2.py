import paho.mqtt.client as mqtt
import redis
from cryptography.fernet import Fernet

broker_address = "localhost"
port = 1883
sensor_topic = "sensor/movimento"
atuador_topic = "atuador/status"
backup_topic = "atuador/status/backup"

# Configurações do Redis
redis_host = "localhost"
redis_port = 6379
redis_db1 = 0  # Número do primeiro banco de dados
redis_db2 = 1  # Número do segundo banco de dados

# Chave Fernet (deve ser a mesma chave usada para criptografar as mensagens)
chave_criptografia = '_t5Xz_HncrR186Vf-h4pFdjnlyhKWcA1gDo1ckeDP3g='.encode()  # Substitua com sua chave
cipher = Fernet(chave_criptografia)

dados_recebidos = 0  # Inicializa o contador de dados recebidos


def connect_redis(db):
    try:
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=db)
        redis_client.ping()  # Check if the server is available
        return redis_client
    except redis.ConnectionError:
        print(f"Falha na conexão com Redis no banco de dados {db}.")
        return None


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao servidor MQTT")
        client.subscribe(sensor_topic)
    else:
        print(f"Falha na conexão ao servidor MQTT com código de retorno {rc}")


def on_message(client, userdata, msg):
    global dados_recebidos
    mensagem_criptografada = msg.payload
    valor_descriptografado = cipher.decrypt(mensagem_criptografada).decode()
    valor = int(valor_descriptografado)

    print(f"Controlador 2 - Valor recebido: {valor}")

    # Tomar decisão com base no valor
    if valor == 1:
        valor_criptografado = cipher.encrypt(str("ativado").encode())
        dados_recebidos += 1
        # Ativar o código do atuador 1 aqui
        client.publish(atuador_topic, valor_criptografado)
        print(f"Controlador 2 - Valor publicado: {valor_criptografado}")
        # Publicar também no tópico de backup
        client.publish(backup_topic, valor_criptografado)

        # Salvar dados no Redis (Banco de dados 1)
        redis_client_db1 = connect_redis(redis_db1)
        if redis_client_db1:
            redis_client_db1.set("movimento_key", valor)

        # Salvar dados no Redis (Banco de dados 2)
        redis_client_db2 = connect_redis(redis_db2)
        if redis_client_db2:
            redis_client_db2.set("movimento_key", valor)


if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker_address, port, 60)

    client.loop_forever()
