import socket
import threading
import json
import time
import queue
import urllib.request

HOST = '0.0.0.0'
CLIENT_PORT = 5000
WORKER_PORT = 5001
SDN_API_URL = 'http://127.0.0.1:8080/sdn/reroute'

job_queue = queue.Queue()
workers = {}         # worker_id -> {'conn': sock, 'load': int, 'last_seen': float, 'active_job': dict}
client_sockets = {}  # job_id -> client_conn
lock = threading.Lock()

def notify_sdn_reroute(reason):
    print(f"[SDN INTEGRATION] Triggering SDN reroute due to: {reason}")
    try:
        req = urllib.request.Request(SDN_API_URL, method='POST')
        with urllib.request.urlopen(req, timeout=2) as resp:
            print(f"[SDN RESPONSE] {resp.read().decode()}")
    except Exception as e:
        print(f"[SDN WARNING] Could not reach SDN REST API: {e}")

def handle_client(conn, addr):
    print(f"[CLIENT CONNECTED] {addr}")
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            for line in data.decode().strip().split('\n'):
                msg = json.loads(line)
                if msg['type'] == 'SUBMIT_JOB':
                    job_id = msg['job_id']
                    with lock:
                        client_sockets[job_id] = conn
                    job_queue.put(msg)
                    ack = json.dumps({'type': 'ACK', 'job_id': job_id, 'status': 'QUEUED'}) + '\n'
                    conn.sendall(ack.encode())
                    print(f"[JOB QUEUED] {job_id} from {addr}")
    except Exception as e:
        print(f"[CLIENT ERROR] {addr}: {e}")
    finally:
        conn.close()

def handle_worker(conn, addr):
    worker_id = None
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            for line in data.decode().strip().split('\n'):
                msg = json.loads(line)
                if msg['type'] == 'REGISTER':
                    worker_id = msg['worker_id']
                    with lock:
                        workers[worker_id] = {
                            'conn': conn, 'load': 0, 'last_seen': time.time(), 'active_job': None
                        }
                    print(f"[WORKER REGISTERED] {worker_id} at {addr}")
                elif msg['type'] == 'HEARTBEAT':
                    with lock:
                        if worker_id in workers:
                            workers[worker_id]['last_seen'] = time.time()
                            workers[worker_id]['load'] = msg.get('load', 0)
                elif msg['type'] == 'JOB_RESULT':
                    job_id = msg['job_id']
                    print(f"[JOB COMPLETED] {job_id} by {worker_id} -> Result: {msg['result']}")
                    with lock:
                        if worker_id in workers:
                            workers[worker_id]['load'] = max(0, workers[worker_id]['load'] - 1)
                            workers[worker_id]['active_job'] = None
                        c_conn = client_sockets.pop(job_id, None)
                    if c_conn:
                        try:
                            res_pkt = json.dumps({'type': 'RESULT', 'job_id': job_id, 'result': msg['result']}) + '\n'
                            c_conn.sendall(res_pkt.encode())
                        except:
                            pass
    except Exception as e:
        print(f"[WORKER DISCONNECTED] {worker_id}: {e}")
    finally:
        if worker_id:
            requeue_worker_job(worker_id)
        conn.close()

def requeue_worker_job(worker_id):
    with lock:
        info = workers.pop(worker_id, None)
    if info:
        print(f"[FAILURE DETECTED] Worker {worker_id} failed!")
        notify_sdn_reroute(f"Worker {worker_id} failure")
        if info['active_job']:
            failed_job = info['active_job']
            print(f"[REDISTRIBUTING] Re-queuing interrupted job {failed_job['job_id']}")
            job_queue.put(failed_job)

def monitor_workers():
    while True:
        time.sleep(2)
        now = time.time()
        dead_workers = []
        with lock:
            for wid, info in workers.items():
                if now - info['last_seen'] > 6.0:
                    dead_workers.append(wid)
        for wid in dead_workers:
            requeue_worker_job(wid)

def scheduler():
    while True:
        job = job_queue.get()
        assigned = False
        while not assigned:
            with lock:
                # Pick active worker with lowest load
                available = [w for w in workers.items() if w[1]['active_job'] is None]
                if available:
                    best_wid, best_info = min(available, key=lambda x: x[1]['load'])
                    try:
                        pkt = json.dumps(job) + '\n'
                        best_info['conn'].sendall(pkt.encode())
                        best_info['load'] += 1
                        best_info['active_job'] = job
                        assigned = True
                        print(f"[SCHEDULED] Job {job['job_id']} -> {best_wid}")
                    except Exception:
                        pass
            if not assigned:
                time.sleep(0.5)

def start_server(port, handler):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, port))
    s.listen(10)
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handler, args=(conn, addr), daemon=True).start()

if __name__ == '__main__':
    print("=== Master Job Queue & SDN Broker Started ===")
    threading.Thread(target=start_server, args=(CLIENT_PORT, handle_client), daemon=True).start()
    threading.Thread(target=start_server, args=(WORKER_PORT, handle_worker), daemon=True).start()
    threading.Thread(target=monitor_workers, daemon=True).start()
    scheduler()
