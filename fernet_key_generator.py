from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()
print(f"Generated Fernet Key: {fernet_key.decode()}")
with open("fernet_key.key", "wb") as key_file:
    key_file.write(fernet_key)