import socket
import json
import threading
import os
import time
import hashlib  # Thư viện để tạo hash

PIECE_SIZE = 512 * 1024  # 512KB

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
        """Tính mã hash cho file — dùng SHA1 của file name (không dùng size) để tránh mismatch khi file chưa tồn tại ở peer khác."""
        return hashlib.sha1(file_name.encode()).hexdigest()

    def send_request(self, event, info_hash):
        """Gửi yêu cầu đến Tracker với sự kiện cụ thể (newline-framed) và chờ 1 dòng JSON trả về."""
        request = {
            "info_hash": info_hash,
            "peer_id": self.peer_id,
            "port": self.port,
            "downloaded": self.downloaded,
            "event": event
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect((self.tracker_host, self.tracker_port))
                sock.sendall((json.dumps(request) + "\n").encode())

                # đọc tới newline
                data = b''
                while not data.endswith(b'\n'):
                    part = sock.recv(4096)
                    if not part:
                        break
                    data += part
                if not data:
                    # Trả về dict trống cho backward compat
                    return {}
                try:
                    return json.loads(data.decode().strip())
                except json.JSONDecodeError:
                    print("Lỗi khi giải mã JSON từ Tracker:", data.decode().strip())
                    return {}
        except Exception as e:
            print("Lỗi kết nối Tracker:", e)
            return {}



    def split_file_to_pieces(self, file_path, out_dir):
        """Split file into piece files and return indices list + piece_hashes."""
        os.makedirs(out_dir, exist_ok=True)
        indices = []
        piece_hashes = []
        with open(file_path, "rb") as f:
            idx = 0
            while True:
                block = f.read(PIECE_SIZE)
                if not block:
                    break
                piece_path = os.path.join(out_dir, f"{idx}.piece")
                with open(piece_path, "wb") as pf:
                    pf.write(block)
                indices.append(idx)
                piece_hashes.append(hashlib.sha1(block).hexdigest())
                idx += 1
        return indices, piece_hashes
    
    def upload(self, file_name):
        """Đăng ký file với Tracker: split -> write metainfo -> register pieces."""
        file_path = os.path.join(self.local_folder, file_name)
        if not os.path.exists(file_path):
            print(f"File '{file_name}' không tồn tại trong thư mục local.")
            return

        # copy original to repo root (optional)
        repo_file = os.path.join(self.repo_folder, file_name)
        with open(file_path, "rb") as src, open(repo_file, "wb") as dst:
            dst.write(src.read())

        # compute info_hash (filename:size recommended)
        info_hash = self.calculate_info_hash(file_name)
        pieces_dir = os.path.join(self.repo_folder, info_hash)
        indices, piece_hashes = self.split_file_to_pieces(repo_file, pieces_dir)

        # write metainfo
        metainfo = {
            "file_name": file_name,
            "piece_size": PIECE_SIZE,
            "pieces": len(indices),
            "info_hash": info_hash,
            "piece_hashes": piece_hashes
        }
        with open(os.path.join(pieces_dir, "metainfo.json"), "w") as mf:
            json.dump(metainfo, mf)

        # register with tracker including list of pieces
        request = {
            "info_hash": info_hash,
            "peer_id": self.peer_id,
            "port": self.port,
            "downloaded": self.downloaded,
            "event": "started",
            "pieces": indices
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.tracker_host, self.tracker_port))
            sock.sendall((json.dumps(request) + "\n").encode())
            try:
                resp = sock.recv(4096).decode()
                print("Tracker response:", resp)
            except:
                pass

        print(f"File '{file_name}' chia thành {len(indices)} pieces. info_hash={info_hash}")

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
        """Kết nối và tải file từ một peer cụ thể (per-piece protocol compatible)."""
        try:
            info_hash = self.calculate_info_hash(file_name)
            pieces_dir = os.path.join(self.repo_folder, info_hash)
            os.makedirs(pieces_dir, exist_ok=True)

            # 1) Yêu cầu metainfo từ peer
            try:
                with socket.socket() as s:
                    s.settimeout(6)
                    s.connect((peer['ip'], peer['port']))
                    s.sendall((json.dumps({"type":"get_metainfo", "info_hash": info_hash}) + "\n").encode())
                    data = b''
                    while not data.endswith(b'\n'):
                        part = s.recv(4096)
                        if not part:
                            break
                        data += part
                    if not data:
                        return False
                    hdr = json.loads(data.decode().strip())
                    if hdr.get("status") != "ok":
                        return False
                    meta = hdr.get("meta")
            except Exception as e:
                print(f"Không lấy được metainfo từ {peer.get('peer_id')}: {e}")
                return False

            total_pieces = int(meta.get("pieces", 0))
            piece_hashes = meta.get("piece_hashes", None)

            # 2) Lặp từng piece mà peer claim (nếu tracker cho biết), hoặc toàn bộ nếu không có claim
            claimed = peer.get("pieces")
            if not claimed:
                claimed = list(range(total_pieces))

            for idx in range(total_pieces):
                if idx not in claimed:
                    continue
                out_path = os.path.join(pieces_dir, f"{idx}.piece")
                if os.path.exists(out_path):
                    # verify hash if available
                    if piece_hashes:
                        got = hashlib.sha1(open(out_path,"rb").read()).hexdigest()
                        if got == piece_hashes[idx]:
                            continue
                        else:
                            os.remove(out_path)
                    else:
                        continue

                # request piece
                try:
                    with socket.socket() as s:
                        s.settimeout(8)
                        s.connect((peer['ip'], peer['port']))
                        s.sendall((json.dumps({"type":"get_piece","info_hash":info_hash,"index":idx}) + "\n").encode())

                        buf = b''
                        while b'\n' not in buf:
                            part = s.recv(4096)
                            if not part:
                                break
                            buf += part
                        if not buf:
                            raise RuntimeError("No header")
                        header_raw, rest = buf.split(b'\n',1)
                        hdr = json.loads(header_raw.decode().strip())
                        if hdr.get("status") != "ok":
                            raise RuntimeError("Peer missing piece")
                        size = int(hdr.get("size",0))
                        with open(out_path, "wb") as f:
                            if rest:
                                f.write(rest)
                            received = len(rest)
                            while received < size:
                                chunk = s.recv(min(4096, size - received))
                                if not chunk:
                                    break
                                f.write(chunk)
                                received += len(chunk)
                        if os.path.getsize(out_path) != size:
                            raise RuntimeError("Incomplete piece")
                except Exception as e:
                    # xóa file không hoàn chỉnh nếu có
                    try: os.remove(out_path)
                    except: pass
                    print(f"Failed fetching piece {idx} from {peer.get('peer_id')}: {e}")
                    return False

                # verify hash if provided
                if piece_hashes:
                    got = hashlib.sha1(open(out_path,"rb").read()).hexdigest()
                    if got != piece_hashes[idx]:
                        print(f"Hash mismatch piece {idx} from {peer.get('peer_id')}")
                        try: os.remove(out_path)
                        except: pass
                        return False

            # 3) kiểm tra có đủ pieces (0..total-1)
            have_all = all(os.path.exists(os.path.join(pieces_dir, f"{i}.piece")) for i in range(total_pieces))
            if not have_all:
                print("Thiếu piece sau khi tải từ peer.")
                return False

            # 4) assemble
            out_filename = f"downloaded_{info_hash}_{meta.get('file_name')}"
            out_path = os.path.join(self.repo_folder, out_filename)
            with open(out_path,"wb") as out:
                for i in range(total_pieces):
                    p = os.path.join(pieces_dir, f"{i}.piece")
                    with open(p,"rb") as pf:
                        out.write(pf.read())
            print(f"Đã tải và ghép file: {out_path}")
            return True

        except Exception as e:
            print(f"Lỗi khi tải từ peer {peer.get('peer_id')}: {e}")
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
        """Gửi file cho một peer yêu cầu. (Framed JSON header protocol)
        Phiên bản này:
        - đọc header JSON tới newline
        - xử lý trường hợp recv trả cả header + một phần payload (kept buffer)
        - dùng timeouts và bắt lỗi gửi/recv
        """
        try:
            peer_socket.settimeout(8.0)  # tránh chờ vô hạn
            # Đọc header tới newline; có thể recv trả thêm payload
            buf = b''
            while b'\n' not in buf:
                part = peer_socket.recv(4096)
                if not part:
                    # kết nối đóng từ client
                    return
                buf += part
                # bảo vệ chống vòng lặp vô hạn
                if len(buf) > 10 * 1024 * 1024:
                    print("Header quá lớn, hủy kết nối.")
                    return

            # tách header và phần dư (nếu có)
            header_raw, rest = buf.split(b'\n', 1)
            try:
                req = json.loads(header_raw.decode().strip())
            except Exception as ex:
                print("Invalid request header:", ex)
                return

            req_type = req.get("type")
            if req_type == "get_metainfo":
                info_hash = req.get("info_hash")
                meta_path = os.path.join(self.repo_folder, info_hash, "metainfo.json")
                if os.path.exists(meta_path):
                    with open(meta_path, "r") as mf:
                        meta = json.load(mf)
                    # trả header và không có payload dữ liệu thêm
                    try:
                        peer_socket.sendall((json.dumps({"status": "ok", "meta": meta}) + "\n").encode())
                    except (BrokenPipeError, ConnectionResetError):
                        return
                else:
                    try:
                        peer_socket.sendall((json.dumps({"status": "missing"}) + "\n").encode())
                    except (BrokenPipeError, ConnectionResetError):
                        return

            elif req_type == "get_piece":
                info_hash = req.get("info_hash")
                index = req.get("index")
                piece_path = os.path.join(self.repo_folder, info_hash, f"{index}.piece")
                if not os.path.exists(piece_path):
                    try:
                        peer_socket.sendall((json.dumps({"status": "missing"}) + "\n").encode())
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return

                size = os.path.getsize(piece_path)
                # gửi header response (size) + newline
                try:
                    peer_socket.sendall((json.dumps({"status": "ok", "size": size}) + "\n").encode())
                except (BrokenPipeError, ConnectionResetError):
                    return

                # nếu có phần payload dư từ lần recv header, gửi phần đó đi trước khi đọc file
                if rest:
                    try:
                        peer_socket.sendall(rest)
                    except (BrokenPipeError, ConnectionResetError):
                        # client đã đóng, thôi
                        return

                # sau đó gửi phần còn lại của piece từ đĩa
                with open(piece_path, "rb") as pf:
                    while True:
                        chunk = pf.read(4096)
                        if not chunk:
                            break
                        try:
                            peer_socket.sendall(chunk)
                        except (BrokenPipeError, ConnectionResetError):
                            # client đóng, dừng gửi
                            return
                print(f"Đã gửi piece {index} của {info_hash} cho peer")
            else:
                try:
                    peer_socket.sendall((json.dumps({"status":"unknown"}) + "\n").encode())
                except (BrokenPipeError, ConnectionResetError):
                    pass

        except socket.timeout:
            print("Timeout khi đọc request từ peer.")
        except Exception as e:
            print(f"Lỗi khi gửi file cho peer: {e}")
        finally:
            try:
                peer_socket.close()
            except:
                pass


    def stop(self, file_name):
        """Ngừng chia sẻ file với Tracker."""
        info_hash = self.calculate_info_hash(file_name)
        response = self.send_request("stopped", info_hash)
        print("Đã ngừng chia sẻ file.")

    def send_keepalive(self):
        """Gửi tín hiệu keepalive định kỳ đến Tracker (kết nối tạm, newline-framed)."""
        while self.is_running:
            time.sleep(self.keepalive_interval)
            keepalive_request = {
                "peer_id": self.peer_id,
                "port": self.port,
                "event": "keepalive"
            }
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(4)
                    s.connect((self.tracker_host, self.tracker_port))
                    s.sendall((json.dumps(keepalive_request) + "\n").encode())
                    # optional read
                    _ = s.recv(1024)
            except Exception as e:
                # không làm crash, chỉ log để debug
                print("Keepalive failed:", e)
    
   
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
