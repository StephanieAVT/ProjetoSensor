import paho.mqtt.client as mqtt
import redis
from cassandra.cluster import Cluster
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

# Configurações do Cassandra
cassandra_contact_points = ['localhost']
cassandra_keyspace = 'sensor_data'  # Substitua com o nome do seu keyspace


# Chave Fernet (deve ser a mesma chave usada para criptografar as mensagens)
chave_criptografia = '_t5Xz_HncrR186Vf-h4pFdjnlyhKWcA1gDo1ckeDP3g='.encode()  # Substitua com sua chave
cipher = Fernet(chave_criptografia)


dados_recebidos = 0  # Inicializa o contador de dados recebidos

def connect_cassandra():
    try:
        cluster = Cluster(contact_points=cassandra_contact_points)
        session = cluster.connect(keyspace=cassandra_keyspace)
        return session
    except Exception as e:
        print(f"Falha na conexão com o Cassandra: {e}")
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

    print(f"Controlador 1 - Valor recebido: {valor}")
    dados_recebidos += 1

    valor_criptografado = cipher.encrypt(str(valor).encode())
    # Ativar o código do atuador 1 aqui
    client.publish(atuador_topic, valor_criptografado)
    print(f"Controlador 1 - Valor publicado: {valor_criptografado}")
    # Publicar também no tópico de backup
    client.publish(backup_topic, valor_criptografado)

    # Salvar dados no Cassandra
    cassandra_session = connect_cassandra()
    if cassandra_session:
        cassandra_session.execute(f"INSERT INTO sensor (movimento_key) VALUES ({valor})")



if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker_address, port, 60)

    client.loop_forever()
