import paramiko

ip = "192.168.6.129"
username = "admin"
password = "cisco"

t = paramiko.Transport((ip, 22))
opts = t.get_security_options()

print("KEX before:", opts.kex)
print("Ciphers before:", opts.ciphers)

opts.kex = [
    "diffie-hellman-group14-sha1",
]

opts.ciphers = ["aes128-cbc"]

print("KEX after:", opts.kex)
print("Ciphers after:", opts.ciphers)

try:
    t.connect(
        username=username,
        password=password
    )

    print("SSH CONNECTED")

except Exception as e:
    print("SSH ERROR:", repr(e))

finally:
    t.close()
