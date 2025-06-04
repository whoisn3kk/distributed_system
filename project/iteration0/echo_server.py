import socket
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - SERVER - %(levelname)s - %(message)s')

def start_server(host='0.0.0.0', port=65432):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        logging.info(f"Echo server listening on {host}:{port}")
        try:
            while True:
                conn, addr = s.accept()
                with conn:
                    logging.info(f"Connected by {addr}")
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            logging.info(f"Connection closed by {addr}")
                            break
                        message = data.decode()
                        logging.info(f"Received from {addr}: {message}")
                        conn.sendall(data) # Echo back
                        logging.info(f"Echoed to {addr}: {message}")
        except KeyboardInterrupt:
            logging.info("Server is shutting down.")
        finally:
            s.close()

if __name__ == '__main__':
    start_server()