import socket
import logging
import signal
import sys

from common.utils import Bet, store_bets, recv_fully
from common.transfer import parse_batch, parse_bet, build_ack_message

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

            agency, bets = parse_batch(payload)
            store_bets(bets)

            logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')

            ack_message = build_ack_message(agency, 0x00)
            client_sock.sendall(ack_message)

            logging.info(f'action: handle_client_connection | result: success | ip: {address[0]}')
        except Exception as e:
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: {len(bets)}')
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
