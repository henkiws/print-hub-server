package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────

const (
	listenAddr     = ":8080"
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 20 * 1024 * 1024 // 20 MB — base64 image bisa besar
)

// ─────────────────────────────────────────────────────────────────────────────
// HUB
// ─────────────────────────────────────────────────────────────────────────────

// Hub menyimpan semua koneksi WebSocket aktif, key = UUID printer
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client // key: client_id (uuid printer)
}

func newHub() *Hub {
	return &Hub{
		clients: make(map[string]*Client),
	}
}

func (h *Hub) register(clientID string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Kalau ada koneksi lama dengan UUID yang sama, tutup dulu
	if old, ok := h.clients[clientID]; ok {
		log.Printf("[Hub] Replacing old connection for client_id=%s", clientID)
		close(old.send)
	}
	h.clients[clientID] = c
	log.Printf("[Hub] Registered client_id=%s (total=%d)", clientID, len(h.clients))
}

func (h *Hub) unregister(clientID string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing, ok := h.clients[clientID]; ok && existing == c {
		delete(h.clients, clientID)
		log.Printf("[Hub] Unregistered client_id=%s (total=%d)", clientID, len(h.clients))
	}
}

// push mengirim pesan JSON ke client yang UUID-nya cocok
func (h *Hub) push(clientID string, msg []byte) bool {
	h.mu.RLock()
	c, ok := h.clients[clientID]
	h.mu.RUnlock()

	if !ok {
		log.Printf("[Hub] push: client_id=%s NOT found", clientID)
		return false
	}

	select {
	case c.send <- msg:
		log.Printf("[Hub] push: sent %d bytes to client_id=%s", len(msg), clientID)
		return true
	default:
		// Buffer penuh, drop & kick
		log.Printf("[Hub] push: buffer full for client_id=%s, dropping", clientID)
		h.unregister(clientID, c)
		close(c.send)
		return false
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CLIENT
// ─────────────────────────────────────────────────────────────────────────────

type Client struct {
	hub      *Hub
	clientID string
	conn     *websocket.Conn
	send     chan []byte
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024 * 1024, // 1 MB write buffer
	CheckOrigin: func(r *http.Request) bool {
		return true // allow semua origin — batasi di production kalau perlu
	},
}

// writePump: kirim pesan dari channel ke WebSocket
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		c.hub.unregister(c.clientID, c)
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Channel ditutup
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("[Client] writePump error client_id=%s: %v", c.clientID, err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump: baca pesan dari client (pong handler + keep-alive)
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister(c.clientID, c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[Client] readPump unexpected close client_id=%s: %v", c.clientID, err)
			}
			break
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP HANDLERS
// ─────────────────────────────────────────────────────────────────────────────

// GET /ws?client_id=<uuid>
// Printer client (.exe) konek ke sini
func wsHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID := r.URL.Query().Get("client_id")
		if clientID == "" {
			http.Error(w, "client_id required", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[WS] Upgrade error: %v", err)
			return
		}

		c := &Client{
			hub:      hub,
			clientID: clientID,
			conn:     conn,
			send:     make(chan []byte, 16), // buffer 16 pesan
		}
		hub.register(clientID, c)

		// Kirim konfirmasi koneksi ke client
		ack, _ := json.Marshal(map[string]string{
			"status":    "connected",
			"client_id": clientID,
		})
		c.send <- ack

		go c.writePump()
		c.readPump() // blocking
	}
}

// POST /api/push-print
// Dipanggil oleh Laravel setelah generate gambar
// Body: JSON dengan field "client_id" wajib ada
func pushPrintHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Baca raw body
		var rawBody json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&rawBody); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Ambil client_id dari payload
		var envelope struct {
			ClientID string `json:"client_id"`
		}
		if err := json.Unmarshal(rawBody, &envelope); err != nil || envelope.ClientID == "" {
			http.Error(w, "client_id missing in payload", http.StatusBadRequest)
			return
		}

		// Forward payload utuh ke WebSocket client
		ok := hub.push(envelope.ClientID, rawBody)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"status":  "error",
				"message": "client not connected: " + envelope.ClientID,
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":    "ok",
			"client_id": envelope.ClientID,
		})
	}
}

// GET /api/clients — debug: lihat siapa yang sedang connect
func listClientsHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hub.mu.RLock()
		ids := make([]string, 0, len(hub.clients))
		for id := range hub.clients {
			ids = append(ids, id)
		}
		hub.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connected_clients": ids,
			"total":             len(ids),
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	hub := newHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(hub))
	mux.HandleFunc("/api/push-print", pushPrintHandler(hub))
	mux.HandleFunc("/api/clients", listClientsHandler(hub))

	log.Printf("[Hub] Starting WebSocket Hub on %s", listenAddr)
	log.Printf("[Hub] WebSocket endpoint  : ws://localhost%s/ws?client_id=<uuid>", listenAddr)
	log.Printf("[Hub] Push endpoint       : http://localhost%s/api/push-print", listenAddr)
	log.Printf("[Hub] Debug clients       : http://localhost%s/api/clients", listenAddr)

	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatal("[Hub] ListenAndServe: ", err)
	}
}