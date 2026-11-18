import socket
import json
import time
import sys

MASTER_IP = '10.0.0.3'
CLIENT_PORT = 5000

def run_client(client_name, num_jobs=3):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    start_time = time.time()
    sock.connect((MASTER_IP, CLIENT_PORT))
    print(f"[{client_name}] Connected to Master at {MASTER_IP}:{CLIENT_PORT}")

    for i in range(1, num_jobs + 1):
        job_id = f"{client_name}-JOB-{i}"
        req = json.dumps({
            'type': 'SUBMIT_JOB',
            'job_id': job_id,
            'payload': [i * 10, i * 20, i * 30]
        }) + '\n'
        t0 = time.time()
        sock.sendall(req.encode())

        # Wait for ACK and RESULT
        received_result = False
        while not received_result:
            data = sock.recv(4096)
            for line in data.decode().strip().split('\n'):
                resp = json.loads(line)
                if resp['type'] == 'ACK':
                    print(f"[{client_name}] Server ACK for {resp['job_id']}: {resp['status']}")
                elif resp['type'] == 'RESULT':
                    rtt = (time.time() - t0) * 1000
                    print(f"[{client_name}] Result for {resp['job_id']} = {resp['result']} (Turnaround: {rtt:.2f} ms)")
                    received_result = True
        time.sleep(1)

    print(f"[{client_name}] All jobs completed in {time.time() - start_time:.2f}s")
    sock.close()

if __name__ == '__main__':
    cname = sys.argv[1] if len(sys.argv) > 1 else 'Client1'
    run_client(cname)
