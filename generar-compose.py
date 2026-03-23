import sys

if len(sys.argv) != 3:
    print("Usage: python3 generar-compose.py <output_file> <client_count>")
    sys.exit(1)

output_file = sys.argv[1]
client_count = int(sys.argv[2])

# Generate the compose file content
compose_content = f"""name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - ./server/config.ini:/config.ini
    networks:
      - testing_net
"""
for i in range(1, client_count + 1):
    compose_content += f"""
  client{i}:
    container_name: client{i}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={i}
      - CLI_NOMBRE=Santiago Lionel
      - CLI_APELLIDO=Lorca
      - CLI_DOCUMENTO=30904465
      - CLI_NACIMIENTO=1999-03-17
      - CLI_NUMERO=7574
    volumes:
      - ./client/config.yaml:/config.yaml
      - ./.data/agency-{i}.csv:/agency.csv
    networks:
      - testing_net
    depends_on:
      - server
"""
    
compose_content += """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

# Write the compose file
with open(output_file, "w") as f:
    f.write(compose_content)