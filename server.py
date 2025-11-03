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

    def handle_client(self, client_socket, client_address):
        """Xử lý yêu cầu từ mỗi client."""
        while True:
            try:
                data = client_socket.recv(1024).decode()
                if not data:
                    break

                # Phân tích yêu cầu của client
                request = json.loads(data)
                event = request.get("event")
                info_hash = request.get("info_hash")
                peer_id = request.get("peer_id")
                port = request.get("port")

                # Khởi tạo `response` với giá trị mặc định
                response = {"status": "unknown_event"}

                # Xử lý từng loại yêu cầu
                if event == "started":
                    self.register_client(info_hash, peer_id,
                                         client_address[0], port)
                elif event == "stopped":
                    self.deregister_client(info_hash, peer_id)
                    response = {"status": "stopped"}
                elif event == "completed":
                    response = {"status": "completed"}
                elif event == "keepalive":
                    response = {"status": "alive"}
                elif event == "get_peers":
                    response = self.get_peers(info_hash, peer_id)
                # Gửi phản hồi cho client
                client_socket.sendall(json.dumps(response).encode())
            except Exception as e:
                print(
                    f"Lỗi trong quá trình xử lý client {client_address}: {e}")
                break

        # Ngắt kết nối và loại bỏ client khi kết thúc phiên
        client_socket.close()

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
