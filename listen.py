import os
import re
import socket

from mysql import connector

udp_port = 16810

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", udp_port))
print(f"Listening on UDP/{udp_port}")

with connector.connect(host="localhost", user="als-use", password=os.getenv('ALS_USE_PWD'), database="als-use") as db:
    print(f"Connected to DB. Keep'em comin' !")
    messages_template = r"^(.+?)\|\|(.+?)\|\|(.+?)$"

    while True:
        data, _ = sock.recvfrom(1024)
        message = data.decode().strip()
        match = re.match(messages_template, message)

        if not match:
            print(f'Received invalid message: "{message}" !!!')
            continue

        print(f'Received message: "{message}"')

        version = match.group(1)
        architecture = match.group(2)
        os = match.group(3)

        db_statement = "insert into STARTS (TIMESTAMP, VERSION, ARCH, OS) values (CURRENT_TIMESTAMP, %s, %s, %s)"
        inserted_values = (version, architecture, os)
        db.cursor().execute(db_statement, inserted_values)
        db.commit()
