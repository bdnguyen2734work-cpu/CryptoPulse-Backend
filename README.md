# 📈 CryptoPulse Backend - Real-time Big Data & AI Crypto Tracker

CryptoPulse Backend là hệ thống lõi cung cấp dữ liệu thời gian thực, phân tích kỹ thuật và tin tức thị trường cho ứng dụng Android CryptoPulse. Hệ thống được thiết kế với kiến trúc luồng dữ liệu (Data Pipeline) mạnh mẽ, tối ưu chi phí bằng cách tận dụng API từ các sàn giao dịch và các dịch vụ Cloud Serverless.

## 🌟 Điểm nổi bật của hệ thống
- **Tối ưu chi phí (Zero-cost Architecture):** Sử dụng trực tiếp WebSocket/API từ các sàn giao dịch (Binance) thay vì các dịch vụ trả phí (CoinMarketCap/CoinGecko).
- **Lưu trữ Serverless:** Tích hợp TiDB Cloud (MySQL-compatible) và Upstash Redis đảm bảo hiệu năng cao, chi phí vận hành $0.
- **Bảo mật cao cấp:** Quản lý Service Account Key của Firebase qua biến môi trường (Environment Variables) mã hóa Base64 trên Render, loại bỏ rủi ro lộ key trên GitHub.
- **Tối ưu băng thông:** Xử lý Avatar trực tiếp từ App lên Cloudinary, Backend chỉ nhận và lưu trữ URL.

## 🛠️ Tech Stack & Architecture

### 🧠 Core Framework & Database
- **FastAPI + Uvicorn:** Xử lý API tốc độ cao, hỗ trợ bất đồng bộ (async).
- **TiDB Cloud (MySQL):** Cơ sở dữ liệu quan hệ chính (Serverless, TLS enforced).
- **Upstash Redis:** Caching dữ liệu API và quản lý kết nối Real-time.

### 🌊 Big Data Pipeline (Data Processing)
- **Apache Kafka:** Message Broker trung chuyển dữ liệu từ các sàn giao dịch.
- **Apache Spark (Structured Streaming):** Xử lý luồng dữ liệu thời gian thực và làm sạch dữ liệu (Clean Data).

### 🔐 Authentication & Cloud
- **Firebase Admin SDK:** Xác thực người dùng (Google Login & Email/Password).
- **Render:** Nền tảng triển khai (Deployment) với file `docker-compose.yml` tích hợp.

## 📡 API Endpoints

| Method | Endpoint | Mô tả | Authorization |
|--------|----------|-------|---------------|
| `POST` | `/api/v1/auth/register` | Đăng ký tài khoản mới | - |
| `POST` | `/api/v1/auth/login` | Đăng nhập hệ thống | - |
| `POST` | `/api/v1/auth/google` | Đăng nhập bằng Google (SSO) | - |
| `POST` | `/api/v1/auth/avatar` | Cập nhật Avatar (Nhận Cloudinary URL) | Bearer Token |
| `GET`  | `/api/v1/market/top-coins` | Lấy danh sách Top 24 Coins | - |
| `GET`  | `/api/v1/market/fear-greed` | Chỉ số Sợ hãi & Tham lam (Fear & Greed Index) | - |
| `GET`  | `/api/v1/history/{symbol}` | Lấy dữ liệu nến (Candlestick) lịch sử | - |
| `GET`  | `/api/v1/analysis/trend/{symbol}` | Phân tích xu hướng thị trường bằng AI | - |
| `GET`  | `/api/v1/news/whale` | Tin tức cá mập (On-chain) dịch tự động sang Tiếng Việt | - |

## 🚀 Hướng dẫn chạy Local (Môi trường Windows/PowerShell)

**1. Clone Repository & Cài đặt môi trường**
```powershell
git clone [https://github.com/bdnguyen2734work-cpu/CryptoPulse-Backend.git](https://github.com/bdnguyen2734work-cpu/CryptoPulse-Backend.git)
cd CryptoPulse-Backend
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt