import socket
import logging
import signal
import sys

from common.utils import Bet, store_bets

def signal_handler(self, signum, frame):
    self.server.running = False
    self.server._server_socket.close()
    for client in self.server.clients:
        client.close()
    logging.info("action: shutdown_server | result: success")
    sys.exit(0)

# Register the signal handler
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def recv_fully(sock, n):
    """Receive exactly n bytes from the socket."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            raise ConnectionError("Socket connection closed")
        data.extend(packet)
    return bytes(data)

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

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.clients = []
        self.running = True

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        while self.running:
            try:
                client_sock = self.__accept_new_connection()
                self.clients.append(client_sock)
                self.__handle_client_connection(client_sock)
            except:
                logging.info("action: shutdown_server | result: in_progress")
                break


    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            address = client_sock.getpeername()
            logging.info(f'action: handle_client_connection | result: in_progress | ip: {address[0]}')

            # Read message length            
            message_length = int.from_bytes(recv_fully(client_sock, 4), byteorder='big')

            # Read message payload
            payload = recv_fully(client_sock, message_length)

            bet = parse_bet(payload)
            store_bets([bet])
            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')

            ack_message = build_ack_message(bet.agency, 0x00)
            client_sock.sendall(ack_message)
            logging.info(f'action: handle_client_connection | result: success | ip: {address[0]}')

        except Exception as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")

            try:
                ack_message = build_ack_message(0, 0x01)
                client_sock.sendall(ack_message)
            except:
                pass
        finally:
            client_sock.close()
            self.clients.remove(client_sock)

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
