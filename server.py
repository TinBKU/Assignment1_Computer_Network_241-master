import socket
import threading
import json


class Tracker:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.files = {}  # Lưu trữ info_hash và các peer có file đó
        self.lock = threading.Lock()  # Dùng để tránh xung đột dữ liệu

    def start(self):
        """Khởi động server và lắng nghe các kết nối từ client."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Tracker đang lắng nghe tại {self.host}:{self.port}")

        # Tạo một thread để xử lý các lệnh từ người dùng
        threading.Thread(target=self.cmd_interface, daemon=True).start()

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Client {client_address} đã kết nối.")
            threading.Thread(target=self.handle_client, args=(
                client_socket, client_address)).start()

    def cmd_interface(self):
        """Giao diện dòng lệnh để kiểm tra trạng thái của Tracker."""
        while True:
            command = input(">> ").strip()
            if command == "list":
                self.print_status()
            elif command == "exit":
                print("Đang dừng Tracker...")
                break
            else:
                print(
                    "Lệnh không hợp lệ. Sử dụng 'list' để xem trạng thái hoặc 'exit' để thoát.")

    def print_status(self):
        """In ra trạng thái hiện tại của các file và các peer chia sẻ trên Tracker."""
        with self.lock:
            if not self.files:
                print("Không có file nào đang được chia sẻ.")
            else:
                print("Danh sách các file và các peer chia sẻ:")
                for info_hash, peers in self.files.items():
                    print(f"File: {info_hash}")
                    for peer in peers:
                        print(
                            f"  - Peer ID: {peer['peer_id']}, IP: {peer['ip']}, Port: {peer['port']}")
                print()

    def handle_client(self, conn, addr):
        try:
            # read until newline (client sends json + "\n")
            data = b''
            while not data.endswith(b'\n'):
                part = conn.recv(4096)
                if not part:
                    break
                data += part
            if not data:
                conn.close(); return

            try:
                req = json.loads(data.decode().strip())
            except Exception as e:
                print("Bad request:", e)
                conn.sendall((json.dumps({"status":"bad_request"}) + "\n").encode())
                conn.close(); return

            event = req.get('event')
            info_hash = req.get('info_hash')
            peer_id = req.get('peer_id')
            port = req.get('port')
            pieces = req.get('pieces', [])

            # default response
            response = {"status":"ok"}

            with self.lock:
                if event == 'started':
                    if not info_hash or not peer_id:
                        response = {"status":"missing_fields"}
                    else:
                        if info_hash not in self.files:
                            self.files[info_hash] = []
                        # remove old entry for same peer_id
                        self.files[info_hash] = [p for p in self.files[info_hash] if p.get('peer_id') != peer_id]
                        entry = {'peer_id': peer_id, 'ip': addr[0], 'port': port, 'pieces': pieces}
                        self.files[info_hash].append(entry)
                        print(f"Registered peer {peer_id} for {info_hash} pieces={len(pieces)}")
                        response = {"status":"registered"}

                elif event == 'get_peers':
                    if not info_hash:
                        response = {"status":"missing_info_hash"}
                    else:
                        peers = []
                        for p in self.files.get(info_hash, []):
                            # do not include the requester itself if peer_id provided
                            if p.get('peer_id') != peer_id:
                                peers.append(p)
                        response = {"status":"ok", "peers": peers}

                elif event == 'stopped':
                    if info_hash in self.files:
                        before = len(self.files[info_hash])
                        self.files[info_hash] = [p for p in self.files[info_hash] if p.get('peer_id') != peer_id]
                        after = len(self.files.get(info_hash, []))
                        print(f"Peer {peer_id} stopped for {info_hash} ({before}->{after})")
                        response = {"status":"stopped"}

                elif event == 'keepalive':
                    # update peer pieces if provided; otherwise mark alive
                    updated = False
                    if info_hash:
                        lst = self.files.setdefault(info_hash, [])
                        for p in lst:
                            if p.get('peer_id') == peer_id:
                                if pieces:
                                    p['pieces'] = pieces
                                p['ip'] = addr[0]; p['port'] = port
                                updated = True
                        if not updated:
                            # if not present, register it
                            entry = {'peer_id': peer_id, 'ip': addr[0], 'port': port, 'pieces': pieces}
                            self.files[info_hash].append(entry)
                            updated = True
                        response = {"status":"alive"}

                else:
                    response = {"status":"unknown_event"}

            # send response (newline-framed)
            conn.sendall((json.dumps(response) + "\n").encode())

        except Exception as e:
            print("Tracker error:", e)
            try:
                conn.sendall((json.dumps({"status":"error","detail":str(e)}) + "\n").encode())
            except:
                pass
        finally:
            conn.close()

    def register_client(self, info_hash, peer_id, ip, port):
        """Đăng ký một client vào tracker, xác nhận client chia sẻ file với info_hash cụ thể."""
        with self.lock:
            if info_hash not in self.files:
                self.files[info_hash] = []

            # Kiểm tra xem client đã đăng ký chưa, tránh trùng lặp
            peer_info = {"peer_id": peer_id, "ip": ip, "port": port}
            if peer_info not in self.files[info_hash]:
                self.files[info_hash].append(peer_info)
                print(
                    f"Đã đăng ký client {peer_id} với info_hash {info_hash}.")

    def deregister_client(self, info_hash, peer_id):
        """Xóa một client khỏi danh sách chia sẻ file với info_hash."""
        with self.lock:
            if info_hash in self.files:
                self.files[info_hash] = [
                    peer for peer in self.files[info_hash] if peer["peer_id"] != peer_id
                ]
                print(f"Đã xóa client {peer_id} với info_hash {info_hash}.")

                # Nếu không còn client nào chia sẻ file này, xóa info_hash khỏi danh sách
                if not self.files[info_hash]:
                    del self.files[info_hash]

    def get_peers(self, info_hash, requesting_peer_id):
        """Lấy danh sách các peer có file với info_hash, loại trừ chính client yêu cầu."""
        with self.lock:
            peers = [
                peer for peer in self.files.get(info_hash, [])
                # Loại trừ client đang yêu cầu
                if peer["peer_id"] != requesting_peer_id
            ]
        return {"peers": peers}


if __name__ == "__main__":
    tracker = Tracker()
    tracker.start()
