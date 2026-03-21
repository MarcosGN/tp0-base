from common.utils import Bet

def parse_bet(payload):
    offset = 0

    message_type = payload[offset]
    offset += 1

    if message_type != 0x01:
        raise ValueError("Invalid message type")
    
    agency = int.from_bytes(payload[offset:offset+4], byteorder='big')
    offset += 4

    nombre_length = int.from_bytes(payload[offset:offset+2], byteorder='big')
    offset += 2
    nombre = payload[offset:offset+nombre_length].decode('utf-8')
    offset += nombre_length

    apellido_length = int.from_bytes(payload[offset:offset+2], byteorder='big')
    offset += 2
    apellido = payload[offset:offset+apellido_length].decode('utf-8')
    offset += apellido_length

    documento = int.from_bytes(payload[offset:offset+8], byteorder='big')
    offset += 8

    nacimiento = payload[offset:offset+10].decode('utf-8')
    offset += 10

    numero = int.from_bytes(payload[offset:offset+4], byteorder='big')
    offset += 4

    return Bet(agency, nombre, apellido, str(documento), nacimiento, str(numero))

def build_ack_message(agency_id, status_code):
    payload = bytearray()
    payload.append(0x02)  # message type
    payload.extend(agency_id.to_bytes(4, byteorder='big'))
    payload.append(status_code)

    length = len(payload).to_bytes(4, byteorder='big')

    return length + payload
