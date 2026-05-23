# 🖨 print-hub

> **WebSocket Push-Print Server** — jembatan antara Laravel dan printer fisik di komputer kasir.

![Go](https://img.shields.io/badge/Go-1.21-00ADD8?style=flat&logo=go) ![WebSocket](https://img.shields.io/badge/WebSocket-Gorilla-4B9CD3) ![Deploy](https://img.shields.io/badge/Deploy-Linux%20VPS-FFA500)

---

## 📋 Daftar Isi

- [Gambaran Umum](#-gambaran-umum)
- [Struktur Folder & File](#-struktur-folder--file)
- [Penjelasan go.mod](#-penjelasan-gomod)
- [Penjelasan hub.go](#-penjelasan-hubgo)
  - [CONFIG](#41--config--konstanta-global)
  - [HUB](#42--hub--registry-koneksi-websocket)
  - [CLIENT](#43--client--representasi-satu-koneksi)
  - [HTTP HANDLERS](#44--http-handlers--endpoint-api)
  - [MAIN](#45--main--entry-point)
- [Build & Compile](#-build--compile)
  - [Dari Windows](#51--compile-dari-windows)
  - [Dari Linux / macOS](#52--compile-dari-linux--macos)
  - [Verifikasi Hasil Build](#53--verifikasi-hasil-build)
- [Deploy ke VPS](#-deploy-ke-vps)
- [Penggunaan API](#-penggunaan-api)
- [Troubleshooting](#-troubleshooting)

---

## 🌐 Gambaran Umum

print-hub adalah server WebSocket ringan yang ditulis dalam Go. Tugasnya satu: menjadi jembatan antara aplikasi web (Laravel) dan printer fisik yang terpasang di komputer kasir/operator.

```
🌐 Laravel  →  ⚙️ print-hub (hub.go)  →  🖥 WebSocket Client (.exe)  →  🖨 Printer
```

**Alur kerja:**
1. Aplikasi printer (`.exe`) di komputer kasir konek ke `/ws?client_id=<UUID>`
2. Laravel memanggil `POST /api/push-print` dengan payload JSON + `client_id`
3. print-hub meneruskan payload ke client yang UUID-nya cocok
4. Client mencetak ke printer lokal

---

## 📁 Struktur Folder & File

```
print-hub/
├── hub.go        ← source code utama (satu file, semua logika)
├── go.mod        ← definisi module & dependency
└── go.sum        ← checksum dependency (auto-generated)
```

| File | Fungsi |
|------|--------|
| `hub.go` | Seluruh logika server: Hub, Client, HTTP handler, `main()` |
| `go.mod` | Nama module, versi Go minimum, dan daftar dependency |
| `go.sum` | Hash kriptografis tiap dependency; dibuat otomatis oleh `go mod tidy` |

---

## 📦 Penjelasan go.mod

```go
module print-hub
go 1.21
require github.com/gorilla/websocket v1.5.1
require golang.org/x/net v0.17.0 // indirect
```

| Baris | Penjelasan |
|-------|------------|
| `module print-hub` | Nama module Go ini. Digunakan sebagai prefix import package internal. |
| `go 1.21` | Versi minimum Go yang dibutuhkan untuk mengkompilasi project. |
| `gorilla/websocket v1.5.1` | Library WebSocket paling populer di ekosistem Go. Menyediakan upgrader dan tipe `Conn` untuk mengelola koneksi WS. |
| `golang.org/x/net // indirect` | Dependency tidak langsung — dibutuhkan oleh `gorilla/websocket`, bukan oleh kode kita sendiri. |

---

## 🔍 Penjelasan hub.go

File ini adalah seluruh server. Kode dibagi menjadi **5 bagian utama** yang dipisahkan oleh komentar pemisah.

---

### 4.1 · CONFIG — Konstanta Global

```go
const (
    listenAddr     = ":8080"
    writeWait      = 10 * time.Second
    pongWait       = 60 * time.Second
    pingPeriod     = (pongWait * 9) / 10
    maxMessageSize = 20 * 1024 * 1024
)
```

| Konstanta | Keterangan |
|-----------|-----------|
| `listenAddr` | Port server. `":8080"` berarti dengarkan di semua interface pada port 8080. |
| `writeWait` | Batas waktu untuk menulis satu pesan ke WebSocket (10 detik). |
| `pongWait` | Jika tidak ada pong dari client dalam 60 detik, koneksi dianggap mati. |
| `pingPeriod` | Server mengirim ping setiap 54 detik (90% dari `pongWait`) untuk menjaga koneksi hidup. |
| `maxMessageSize` | Ukuran maksimal pesan dari client: 20 MB (cukup untuk gambar base64 besar). |

---

### 4.2 · HUB — Registry Koneksi WebSocket

Hub adalah pusat kendali. Menyimpan semua koneksi aktif dalam `map`, di mana key-nya adalah UUID printer.

```go
type Hub struct {
    mu      sync.RWMutex
    clients map[string]*Client  // key: client_id (UUID printer)
}
```

| Method / Field | Penjelasan |
|----------------|-----------|
| `mu` (RWMutex) | Kunci baca-tulis untuk melindungi map dari race condition (akses bersamaan dari goroutine berbeda). |
| `register()` | Mendaftarkan client baru. Jika UUID yang sama sudah ada, koneksi lama ditutup terlebih dahulu _(satu printer = satu koneksi)_. |
| `unregister()` | Menghapus client dari map saat koneksi putus. Mengecek kesamaan pointer agar tidak salah hapus. |
| `push()` | Mencari client berdasarkan UUID dan mengirim payload JSON ke channel `send`-nya. Jika buffer penuh, client di-kick. |

---

### 4.3 · CLIENT — Representasi Satu Koneksi

```go
type Client struct {
    hub      *Hub
    clientID string
    conn     *websocket.Conn
    send     chan []byte
}
```

Setiap koneksi WebSocket direpresentasikan oleh satu struct `Client` dengan **dua goroutine** yang berjalan bersamaan:

| Goroutine | Penjelasan |
|-----------|-----------|
| `writePump()` | Membaca dari channel `send` dan mengirimkannya ke WebSocket. Juga mengirim ping secara periodik. Berjalan di goroutine terpisah (`go c.writePump()`). |
| `readPump()` | Membaca pesan masuk dari client (untuk menerima pong). Berjalan _blocking_ di goroutine handler. Jika error/putus, memicu cleanup. |
| `send` (channel) | Buffer 16 pesan. Pesan dari `hub.push()` dimasukkan ke sini, lalu diambil `writePump()` untuk dikirim. |

---

### 4.4 · HTTP HANDLERS — Endpoint API

#### `GET /ws?client_id=<uuid>`
Digunakan oleh **aplikasi printer (`.exe`)** di komputer kasir untuk membuka koneksi WebSocket persisten. Parameter `client_id` wajib ada dan berisi UUID unik printer tersebut.

#### `POST /api/push-print`
Digunakan oleh **Laravel** untuk mengirim perintah cetak. Body berupa JSON yang **wajib** memiliki field `client_id`. Seluruh payload diteruskan ke client yang UUID-nya cocok.

```json
{
    "client_id": "uuid-printer-kasir-1",
    "type":      "print_receipt",
    "image_b64": "data:image/png;base64,iVBOR..."
}
```

#### `GET /api/clients`
Endpoint debug untuk melihat daftar UUID printer yang sedang terhubung. Berguna untuk troubleshooting.

---

### 4.5 · MAIN — Entry Point

Fungsi `main()` melakukan tiga hal:
1. Membuat instance Hub baru
2. Mendaftarkan 3 handler ke HTTP mux (router bawaan Go)
3. Menjalankan HTTP server yang blocking di port 8080

---

## 🔨 Build & Compile

> **Strategi utama:** Kompilasi di laptop lokal → hasilnya satu file binary Linux → upload ke VPS. VPS **tidak perlu** menginstal Go compiler.

Go mendukung cross-compilation secara native — cukup set dua environment variable: `GOOS` dan `GOARCH`.

---

### 5.1 · Compile dari Windows

**Cara 1 — PowerShell:**
```powershell
# Masuk ke folder project
cd C:\path\to\print-hub

# Download dependency
go mod tidy

# Set target OS & arsitektur, lalu compile
$env:GOOS="linux"
$env:GOARCH="amd64"
go build -o print-hub-linux ./...
```

**Cara 2 — Command Prompt (satu baris):**
```cmd
set GOOS=linux&& set GOARCH=amd64&& go build -o print-hub-linux ./...
```

**Cara 3 — Git Bash / WSL:**
```bash
cd /c/path/to/print-hub
go mod tidy
GOOS=linux GOARCH=amd64 go build -o print-hub-linux ./...
```

**Penjelasan flag:**

| Flag / Env Var | Penjelasan |
|----------------|-----------|
| `GOOS=linux` | Target operating system: Linux (bukan Windows tempat kita compile) |
| `GOARCH=amd64` | Target arsitektur: 64-bit x86 (standar VPS modern) |
| `-o print-hub-linux` | Nama file output binary |
| `./...` | Kompilasi semua package dalam module ini |

---

### 5.2 · Compile dari Linux / macOS

```bash
cd /path/to/print-hub
go mod tidy

# Compile untuk Linux amd64 (target VPS)
GOOS=linux GOARCH=amd64 go build -o print-hub-linux ./...

# Jika laptop sudah Linux amd64, bisa langsung:
go build -o print-hub-linux ./...
```

---

### 5.3 · Verifikasi Hasil Build

```bash
# Cek file terbuat
ls -lh print-hub-linux

# Verifikasi tipe file
file print-hub-linux

# Output yang diharapkan:
# print-hub-linux: ELF 64-bit LSB executable, x86-64, ...
```

---

## 🚀 Deploy ke VPS

### Upload Binary

```bash
# Upload dengan SCP
scp print-hub-linux user@IP_VPS:/opt/print-hub/

# Atau dengan rsync
rsync -avz print-hub-linux user@IP_VPS:/opt/print-hub/
```

### Persiapan di VPS

```bash
# Login ke VPS
ssh user@IP_VPS

# Buat direktori & beri izin eksekusi
sudo mkdir -p /opt/print-hub
chmod +x /opt/print-hub/print-hub-linux

# Test jalankan (Ctrl+C untuk stop)
/opt/print-hub/print-hub-linux
```

### Setup Systemd Service (Rekomendasi)

Agar server otomatis berjalan saat VPS reboot:

```bash
sudo nano /etc/systemd/system/print-hub.service
```

Isi file:

```ini
[Unit]
Description=print-hub WebSocket Server
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/print-hub
ExecStart=/opt/print-hub/print-hub-linux
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Aktifkan:

```bash
sudo systemctl daemon-reload
sudo systemctl enable print-hub
sudo systemctl start print-hub

# Cek status
sudo systemctl status print-hub

# Lihat log real-time
sudo journalctl -u print-hub -f
```

---

## 📡 Penggunaan API

### Koneksi WebSocket (Client Printer)

```
ws://IP_VPS:8080/ws?client_id=<UUID_PRINTER>
```

Setelah konek, server mengirim acknowledgement:

```json
{"status": "connected", "client_id": "uuid-printer-kasir-1"}
```

### Push Perintah Cetak (dari Laravel)

```bash
curl -X POST http://IP_VPS:8080/api/push-print \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "uuid-printer-kasir-1",
    "type": "print_receipt",
    "image_b64": "data:image/png;base64,..."
  }'

# Response sukses (200):
{"status": "ok", "client_id": "uuid-printer-kasir-1"}

# Response gagal — printer tidak konek (404):
{"status": "error", "message": "client not connected: uuid-printer-kasir-1"}
```

### Cek Printer yang Terhubung

```bash
curl http://IP_VPS:8080/api/clients

# Response:
{
  "connected_clients": ["uuid-1", "uuid-2"],
  "total": 2
}
```

---

## 🔧 Troubleshooting

| Masalah | Solusi |
|---------|--------|
| Port 8080 tidak bisa diakses | Buka port di firewall: `sudo ufw allow 8080/tcp` |
| Binary tidak bisa dieksekusi | Pastikan sudah `chmod +x`. Cek file tidak corrupt saat upload. |
| Client tidak menerima pesan | Cek `/api/clients` apakah UUID terdaftar. Pastikan `client_id` di Laravel dan printer sama persis. |
| Service langsung berhenti | Cek log: `sudo journalctl -u print-hub --no-pager \| tail -50` |
| `exec format error` | Binary dikompilasi untuk arsitektur yang salah. Compile ulang dengan `GOOS=linux GOARCH=amd64`. |

---

## ✅ Deploy Checklist

```
[ ] go mod tidy                                          (di laptop)
[ ] GOOS=linux GOARCH=amd64 go build -o print-hub-linux ./...
[ ] scp print-hub-linux user@VPS:/opt/print-hub/
[ ] chmod +x /opt/print-hub/print-hub-linux
[ ] Setup systemd service → systemctl enable & start print-hub
[ ] Buka port 8080 di firewall VPS
[ ] Test: curl http://VPS:8080/api/clients
```