import os
import re
import socket

from mysql import connector

udp_port = 16810

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", udp_port))
print(f"Listening on UDP/{udp_port}")

messages_template = r"^(.+?)\|\|(.+?)\|\|(.+?)$"
db_password = os.getenv('ALS_USE_PWD')

while True:
    data, _ = sock.recvfrom(1024)

    try:
        message = data.decode().strip()
    except UnicodeDecodeError:
        print("Invalid Unicode data received. IGNORED")
        continue

    match = re.match(messages_template, message)

    if not match:
        print(f'Received invalid message: "{message}" !!!')
        continue

    print(f'Received message: "{message}"')

    with connector.connect(host="localhost",
                           user="als-use",
                           password=db_password,
                           database="als-use") as db:
        version = match.group(1)
        architecture = match.group(2)
        os = match.group(3)

        db_statement = "insert into STARTS (TIMESTAMP, VERSION, ARCH, OS) values (CURRENT_TIMESTAMP, %s, %s, %s)"
        inserted_values = (version, architecture, os)
        db.cursor().execute(db_statement, inserted_values)
        db.commit()
