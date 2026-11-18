import socket
import threading
import json
import time
import sys

MASTER_IP = '10.0.0.3'
WORKER_PORT = 5001

def send_heartbeats(sock, worker_id, state):
    while True:
        time.sleep(2)
        try:
            hb = json.dumps({'type': 'HEARTBEAT', 'worker_id': worker_id, 'load': state['load']}) + '\n'
            sock.sendall(hb.encode())
        except:
            break

def run_worker(worker_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((MASTER_IP, WORKER_PORT))
    reg = json.dumps({'type': 'REGISTER', 'worker_id': worker_id}) + '\n'
    sock.sendall(reg.encode())
    print(f"[{worker_id}] Connected and registered with Master.")

    state = {'load': 0}
    threading.Thread(target=send_heartbeats, args=(sock, worker_id, state), daemon=True).start()

    while True:
        data = sock.recv(4096)
        if not data:
            break
        for line in data.decode().strip().split('\n'):
            job = json.loads(line)
            if job['type'] == 'SUBMIT_JOB':
                state['load'] = 1
                print(f"[{worker_id}] Executing Job {job['job_id']} (Task: {job['payload']})...")
                time.sleep(4)  # Simulate computation
                result = sum(job['payload'])
                res_pkt = json.dumps({
                    'type': 'JOB_RESULT', 'job_id': job['job_id'], 'result': result
                }) + '\n'
                sock.sendall(res_pkt.encode())
                state['load'] = 0
                print(f"[{worker_id}] Finished Job {job['job_id']}.")

if __name__ == '__main__':
    wid = sys.argv[1] if len(sys.argv) > 1 else 'Worker-1'
    run_worker(wid)
