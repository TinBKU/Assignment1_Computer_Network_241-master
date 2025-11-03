import socket
import json
import threading
import os
import time
import hashlib  # Thư viện để tạo hash


class Client:
    def __init__(self, tracker_host='localhost', tracker_port=5000, peer_id="peer0", port=6000, client_folder="client1"):
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.peer_id = peer_id
        self.port = port
        self.downloaded = 0
        self.peers = []
        self.keepalive_interval = 60  # Khoảng thời gian giữa các tín hiệu keepalive
        self.is_running = True
        self.client_folder = client_folder  # Thư mục dành riêng cho client

        # Khởi tạo thư mục nếu chưa tồn tại
        self.local_folder = os.path.join(self.client_folder, "local")
        self.repo_folder = os.path.join(self.client_folder, "repo")

        if not os.path.exists(self.local_folder):
            os.makedirs(self.local_folder)
        if not os.path.exists(self.repo_folder):
            os.makedirs(self.repo_folder)

        # Tạo socket để lắng nghe yêu cầu từ các peer khác
        self.upload_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.upload_socket.bind(('localhost', self.port))
        self.upload_socket.listen(5)
        print(
            f"{self.peer_id} đang lắng nghe các yêu cầu tải xuống tại cổng {self.port}.")

        # Tạo socket lâu dài cho kết nối keepalive
        self.keepalive_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self.keepalive_socket.connect((self.tracker_host, self.tracker_port))
        print(
            f"{self.peer_id} đã kết nối keepalive với Tracker tại cổng {self.tracker_port}.")

    def calculate_info_hash(self, file_name):
        """Tính mã hash cho tên file dùng hàm SHA-1."""
        return hashlib.sha1(file_name.encode()).hexdigest()

    def send_request(self, event, info_hash):
        """Gửi yêu cầu đến Tracker với sự kiện cụ thể."""
        request = {
            "info_hash": info_hash,
            "peer_id": self.peer_id,
            "port": self.port,
            "downloaded": self.downloaded,
            "event": event
        }

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.tracker_host, self.tracker_port))
            sock.sendall(json.dumps(request).encode())

            # Nhận phản hồi từ Tracker
            response = sock.recv(1024).decode()

            # Kiểm tra xem response có dữ liệu hay không
            if not response:
                print("Không nhận được phản hồi từ Tracker hoặc nhận phản hồi rỗng.")
                return {}  # Trả về một dictionary trống để tránh lỗi

            try:
                return json.loads(response)
            except json.JSONDecodeError:
                print(f"Lỗi khi giải mã JSON: {response}")
                return {}  # Trả về dictionary trống nếu JSON không hợp lệ

    def upload(self, file_name):
        """Đăng ký file với Tracker để chia sẻ."""
        file_path = os.path.join(self.local_folder, file_name)

        if not os.path.exists(file_path):
            print(f"File '{file_name}' không tồn tại trong thư mục local.")
            return

        # Di chuyển file từ thư mục local sang repo để chia sẻ
        with open(file_path, "rb") as src, open(os.path.join(self.repo_folder, file_name), "wb") as dst:
            dst.write(src.read())

        # Tính info_hash từ tên file
        info_hash = self.calculate_info_hash(file_name)
        print("Hash của file:", info_hash)
        response = self.send_request("started", info_hash)
        self.peers = response.get("peers", [])
        print(f"File '{file_name}' đã được đăng ký để chia sẻ với các peer.")

    def download(self, file_name):
        """Tải file từ các peer được cung cấp bởi Tracker."""

        # Kiểm tra xem file đã có trong thư mục 'repo' chưa
        file_path = os.path.join(self.repo_folder, file_name)
        if os.path.exists(file_path):
            print(f"File '{file_name}' đã có sẵn trong thư mục của bạn.")
            return

        # Tính info_hash từ tên file
        info_hash = self.calculate_info_hash(file_name)

        # Gửi yêu cầu tới Tracker để lấy danh sách peer có file
        response = self.send_request("get_peers", info_hash)
        self.peers = response.get("peers", [])

        # Kiểm tra nếu không có peer nào có file
        if not self.peers:
            print(f"Không có peer nào có file '{file_name}'.")
            return

        # Nếu có peer, tiến hành tải file
        for peer in self.peers:
            if self.download_from_peer(peer, file_name):
                print(
                    f"Tải thành công file '{file_name}' từ peer {peer['peer_id']}.")
                # Đăng ký trạng thái hoàn tất tải với Tracker
                self.send_request("completed", info_hash)
                break
            else:
                print(
                    f"Không thể tải file từ peer {peer['peer_id']}. Thử lại với peer khác...")

        # Nếu không tải thành công từ bất kỳ peer nào, thông báo lỗi
        else:
            print(f"Không thể tải file '{file_name}' từ bất kỳ peer nào.")

    def download_from_peer(self, peer, file_name):
        """Kết nối và tải file từ một peer cụ thể."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((peer['ip'], peer['port']))
                sock.sendall(file_name.encode())  # Yêu cầu file từ peer

                # Nhận phần dữ liệu đầu tiên và kiểm tra nếu là lỗi
                first_chunk = sock.recv(1024)
                if first_chunk == b"ERROR: File not found":
                    print(
                        f"Peer {peer['peer_id']} không có file '{file_name}'.")
                    return False

                # Mở file và bắt đầu ghi khi không có lỗi
                with open(os.path.join(self.local_folder, file_name), "wb") as f:
                    f.write(first_chunk)

                    # Tiếp tục nhận dữ liệu và ghi vào file
                    while True:
                        data = sock.recv(1024)
                        if not data:
                            break
                        f.write(data)

            return True
        except Exception as e:
            print(f"Lỗi khi tải từ peer {peer['peer_id']}: {e}")
            return False

    def handle_peer_requests(self):
        """Lắng nghe các yêu cầu tải file từ các peer khác."""
        while self.is_running:
            try:
                peer_socket, peer_address = self.upload_socket.accept()
                print(f"Đang xử lý yêu cầu tải từ {peer_address}")
                threading.Thread(target=self.send_file_to_peer,
                                 args=(peer_socket,)).start()
            except Exception as e:
                print(f"Lỗi trong quá trình xử lý yêu cầu từ peer: {e}")

    def send_file_to_peer(self, peer_socket):
        """Gửi file cho một peer yêu cầu."""
        try:
            file_name = peer_socket.recv(1024).decode()
            file_path = os.path.join(self.repo_folder, file_name)

            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    while True:
                        data = f.read(1024)
                        if not data:
                            break
                        peer_socket.sendall(data)
                print(f"Đã gửi file '{file_name}' thành công.")
            else:
                peer_socket.sendall(b"ERROR: File not found.")
        except Exception as e:
            print(f"Lỗi khi gửi file cho peer: {e}")
        finally:
            peer_socket.close()

    def stop(self, file_name):
        """Ngừng chia sẻ file với Tracker."""
        info_hash = self.calculate_info_hash(file_name)
        response = self.send_request("stopped", info_hash)
        print("Đã ngừng chia sẻ file.")

    def send_keepalive(self):
        """Gửi tín hiệu keepalive định kỳ đến Tracker qua kết nối lâu dài."""
        while self.is_running:
            time.sleep(self.keepalive_interval)
            keepalive_request = {
                "peer_id": self.peer_id,
                "port": self.port,
                "event": "keepalive"
            }
            self.keepalive_socket.sendall(
                json.dumps(keepalive_request).encode())
            print("Gửi tín hiệu keepalive đến Tracker.")

    def cmd_interface(self):
        """Giao diện dòng lệnh cho client."""
        # Tạo thread keepalive
        keepalive_thread = threading.Thread(
            target=self.send_keepalive, daemon=True)
        keepalive_thread.start()

        # Tạo thread lắng nghe yêu cầu tải file từ các peer
        listen_thread = threading.Thread(
            target=self.handle_peer_requests, daemon=True)
        listen_thread.start()

        while True:
            print("\nChọn hành động:")
            print("1. Upload file")
            print("2. Download file")
            print("3. Stop sharing")
            print("4. Thoát")
            choice = input("Nhập lựa chọn của bạn (1-4): ")

            if choice == "1":
                file_name = input("Nhập tên file cần upload: ")
                self.upload(file_name)
            elif choice == "2":
                file_name = input("Nhập tên file cần download: ")
                self.download(file_name)
            elif choice == "3":
                file_name = input("Nhập tên file cần dừng chia sẻ: ")
                self.stop(file_name)
            elif choice == "4":
                print("Đang thoát...")
                self.is_running = False
                self.stop(file_name)
                break
            else:
                print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")


if __name__ == "__main__":
    client = Client(peer_id="peer2", port=6002, client_folder="client2")
    client.cmd_interface()
