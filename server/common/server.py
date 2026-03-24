import socket
import logging
import signal
import sys
import os

from common.utils import load_bets, has_won, store_bets, recv_fully
from common.transfer import parse_batch, build_ack_message, parse_finished, parse_query, build_winners_message

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
        self.clients_amount = int(os.getenv("CLIENTS", "5"))
        self.clients = []
        self.running = True
        self._finished_agencies = set()   
        self._sorteo_listo = False
        self._winners_by_agency = {} 

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
        try:
            while True:
                address = client_sock.getpeername()
                logging.info(f'action: handle_client_connection | result: in_progress | ip: {address[0]}')
 
                length_bytes = recv_fully(client_sock, 4)
                message_length = int.from_bytes(length_bytes, byteorder='big')
                payload = recv_fully(client_sock, message_length)
    
                msg_type = payload[0]
    
                if msg_type == 0x03:
                    self._handle_batch(client_sock, payload, address)
    
                elif msg_type == 0x04:
                    self._handle_finished(client_sock, payload, address)
    
                elif msg_type == 0x05:
                    self._handle_query(client_sock, payload, address)
    
                else:
                    logging.error(f"action: unknown_message | result: fail | ip: {address[0]} | type: {msg_type:#04x}")
                    
        except ConnectionError:
            logging.info(f'action: client_disconnected | result: success | ip: {address[0]}')    
        except Exception as e:
            logging.error(f"action: handle_client | result: fail | ip: {address[0]} | error: {e}")
        finally:
            client_sock.close()
            if client_sock in self.clients:
                self.clients.remove(client_sock)

    def _handle_batch(self, client_sock, payload, address):
        agency, bets = parse_batch(payload)
        store_bets(bets)
        logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
        ack = build_ack_message(agency, 0x00)
        client_sock.sendall(ack)
 
    def _handle_finished(self, client_sock, payload, address):
        agency = parse_finished(payload)
        self._finished_agencies.add(agency)
        logging.info(
            f"action: fin_apuestas | result: success | agency: {agency} "
            f"| total_finished: {len(self._finished_agencies)}"
        )
        ack = build_ack_message(agency, 0x00)
        client_sock.sendall(ack)

        if len(self._finished_agencies) == self.clients_amount and not self._sorteo_listo:
            self._realizar_sorteo()
 
    def _realizar_sorteo(self):
        self._winners_by_agency = {}
        for bet in load_bets():
            if has_won(bet):
                self._winners_by_agency.setdefault(bet.agency, []).append(bet.document)
        self._sorteo_listo = True
        logging.info("action: sorteo | result: success")
 
    def _handle_query(self, client_sock, payload, address):
        agency = parse_query(payload)
 
        if not self._sorteo_listo:
            logging.info(f"action: consulta_ganadores | result: pending | agency: {agency}")
            response = build_winners_message(agency, [], ready=False)
        else:
            winners = self._winners_by_agency.get(agency, [])
            response = build_winners_message(agency, winners, ready=True)
 
        client_sock.sendall(response)
 

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
