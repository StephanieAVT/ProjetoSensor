from cryptography.fernet import Fernet
nova_chave = Fernet.generate_key()
print(f"Chave gerada: {nova_chave.decode()}")

