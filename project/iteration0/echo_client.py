import socket
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - CLIENT - %(levelname)s - %(message)s')

def send_message(host='127.0.0.1', port=65432, message="Hello, Echo Server!"):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            logging.info(f"Sending: '{message}'")
            s.sendall(message.encode())
            data = s.recv(1024)
            logging.info(f"Received echo: '{data.decode()}'")
    except ConnectionRefusedError:
        logging.error(f"Connection refused. Is the server running on {host}:{port}?")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    send_message(message="Test Message 1 from client")
    time.sleep(1)
    send_message(message="Another Message from client")