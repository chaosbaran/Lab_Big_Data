import socket
import cv2
import base64
import time
import json

HOST = "localhost"
PORT = 6100

def start_server():
    # Socket Server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Camera Server đang chờ kết nối tại {HOST}:{PORT}...")

    conn, addr = s.accept()
    print(f"Connected by {addr}")

    # Camera
    cap = cv2.VideoCapture(0) # 0 là webcam, hoặc điền đường dẫn file video

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break

            # Resize
            frame = cv2.resize(frame, (640, 480))

            # Encode ảnh từ .jpg sang base64
            _, buffer = cv2.imencode('.jpg', frame)
            jpg_as_text = base64.b64encode(buffer).decode('utf-8')

            # Gửi dữ liệu
            timestamp = str(time.time())
            message = json.dumps({"timestamp": timestamp, "image": jpg_as_text}) + "\n" # Thêm \n để hết dòng dữ liệu
            
            conn.sendall(message.encode('utf-8'))
            
            print(f"Sent frame: {timestamp}")
            time.sleep(0.1) # Giới hạn FPS

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cap.release()
        conn.close()
        s.close()

if __name__ == "__main__":
    start_server()