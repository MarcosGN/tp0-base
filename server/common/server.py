import socket
import logging
import signal
import sys
import os
import multiprocessing

from common.utils import load_bets, has_won, store_bets, recv_fully
from common.transfer import parse_batch, build_ack_message, parse_finished, parse_query, build_winners_message

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.running = True
        self._processes = []
        self.clients_amount = int(os.getenv("CLIENTS", "5"))
        manager = multiprocessing.Manager()
        self._finished_agencies = manager.list()
        self._finished_agencies_lock = manager.Lock()
        self._sorteo_listo = manager.Event()
        self._winners_by_agency = manager.dict()
        self._storage_lock = manager.Lock()

        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        logging.info("action: shutdown_server | result: in_progress")
        self.running = False
        self._server_socket.close()
        for p in self._processes:
            if p.is_alive():
                p.terminate()
                p.join()
        logging.info("action: shutdown_server | result: success")
        sys.exit(0)

    def run(self):
        while self.running:
            try:
                client_sock = self.accept_new_connection()
                p = multiprocessing.Process(target=handle_client_connection, args=(client_sock, self._finished_agencies, self._finished_agencies_lock, self._sorteo_listo, self._winners_by_agency, self._storage_lock, self.clients_amount))
                p.start()
                client_sock.close()
                self._processes.append(p)
                self._processes = [p for p in self._processes if p.is_alive()]
            except OSError:
                logging.info("action: shutdown_server | result: in_progress")
                break

    def accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    
def handle_client_connection(client_sock, finished_agencies, finished_agencies_lock, sorteo_listo, winners_by_agency, storage_lock, clients_amount):
    try:
        while True:
            address = client_sock.getpeername()
            logging.info(f'action: handle_client_connection | result: in_progress | ip: {address[0]}')

            length_bytes = recv_fully(client_sock, 4)
            message_length = int.from_bytes(length_bytes, byteorder='big')
            payload = recv_fully(client_sock, message_length)

            msg_type = payload[0]

            if msg_type == 0x03:
                handle_batch(client_sock, payload, storage_lock)
            elif msg_type == 0x04:
                handle_finished(client_sock, payload, finished_agencies, finished_agencies_lock, sorteo_listo, winners_by_agency, storage_lock, clients_amount)
            elif msg_type == 0x05:
                handle_query(client_sock, payload, sorteo_listo, winners_by_agency)
            else:
                logging.error(f"action: unknown_message | result: fail | ip: {address[0]} | type: {msg_type:#04x}")
    except ConnectionError:
        logging.info(f"action: client_disconnected | result: success | ip: {address[0]}")
    except Exception as e:
        logging.error(f"action: handle_client | result: fail | ip: {address[0]} | error: {e}")
    finally:
        client_sock.close()

def handle_batch(client_sock, payload, storage_lock):
    agency, bets = parse_batch(payload)
    with storage_lock:
        store_bets(bets)
    logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
    ack = build_ack_message(agency, 0x00)
    client_sock.sendall(ack)

def handle_finished(client_sock, payload, finished_agencies, finished_agencies_lock, sorteo_listo, winners_by_agency, storage_lock, clients_amount):
    agency = parse_finished(payload)
    with finished_agencies_lock:
        finished_agencies.append(agency)
        current_count = len(finished_agencies)
    logging.info(f"action: fin_apuestas | result: success | agency: {agency} | total_finished: {current_count}")
    ack = build_ack_message(agency, 0x00)
    client_sock.sendall(ack)
    if current_count == clients_amount and not sorteo_listo.is_set():
        realizar_sorteo(sorteo_listo, winners_by_agency, storage_lock)
 
def realizar_sorteo(sorteo_listo, winners_by_agency, storage_lock):
    if sorteo_listo.is_set():
        return
    with storage_lock:
        result = {}
        for bet in load_bets():
            if has_won(bet):
                key = str(bet.agency)
                if key not in result:
                    result[key] = []
                result[key].append(bet.document)
        winners_by_agency.update(result)
    sorteo_listo.set()
    logging.info("action: sorteo | result: success")

def handle_query(client_sock, payload, sorteo_listo, winners_by_agency):
    agency = parse_query(payload)

    if not sorteo_listo.is_set():
        logging.info(f"action: sorteo_pendiente | result: pending | agency: {agency}")
        response = build_winners_message(agency, [], ready=False)
    else:
        winners = winners_by_agency.get(str(agency), [])
        logging.info(f"action: ganadores_agencia | result: success | agency: {agency} | cant_ganadores: {len(winners)}")
        response = build_winners_message(agency, winners, ready=True)

    client_sock.sendall(response)

