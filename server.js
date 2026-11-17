const WebSocket = require("ws");

const wss = new WebSocket.Server({ host: "0.0.0.0", port: 8080 });
const clients = new Map();     // userId -> socket
const userLastSeen = {};       // userId -> epoch ms

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────
function send(socket, obj) {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(obj));
    }
}

function isOnline(userId) {
    const s = clients.get(userId);
    return !!(s && s.readyState === WebSocket.OPEN);
}

function broadcastPresence(userId, online, lastSeen) {
    const payload = JSON.stringify({
        type: "presence",
        userId,
        online,
        lastSeen
    });
    wss.clients.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN && ws.userId) {
            ws.send(payload);
        }
    });
}

// ─────────────────────────────────────────────────────────────────────
// Connection
// ─────────────────────────────────────────────────────────────────────
wss.on("connection", (socket, req) => {
    socket.isAlive = true;
    socket.on("pong", () => { socket.isAlive = true; });

    console.log("Client connected:", req.socket.remoteAddress);

    socket.on("message", (raw) => {
        let data;
        try {
            data = JSON.parse(raw.toString());
        } catch (e) {
            console.error("Invalid JSON:", e.message);
            return;
        }

        // ── register ──────────────────────────────────────────────
        if (data.type === "register") {
            const id = String(data.userId || "").trim();
            if (!id) return;

            const old = clients.get(id);
            if (old && old !== socket) {
                // old socket is being replaced by a new one for the same user.
                // Don't broadcast "offline" — the user is still online, just on
                // a new connection.
                old.userId = null;
                old.close(4000, "replaced by new connection");
            }

            socket.userId = id;
            clients.set(id, socket);
            userLastSeen[id] = Date.now();

            console.log("Registered:", id);
            send(socket, { type: "registered", userId: id });

            // ✅ Tell everyone this user is now online
            broadcastPresence(id, true, null);
            return;
        }

        // All other message types require registration first
        if (!socket.userId) {
            send(socket, { type: "error", message: "Not registered" });
            return;
        }

        // ── subscribePresence ────────────────────────────────────
        if (data.type === "subscribePresence") {
            const target = String(data.userId || "").trim();
            if (!target) return;

            send(socket, {
                type: "presence",
                userId: target,
                online: isOnline(target),
                lastSeen: userLastSeen[target] || null
            });
            return;
        }

        // ── forward (offer / answer / candidate / hangup / reject) ──
        const receiver = clients.get(data.to);
        if (!receiver || receiver.readyState !== WebSocket.OPEN) {
            console.log("Receiver offline:", data.to, "(" + data.type + ")");

            // Only an OFFER needs an error. Dropping late candidates / hangups
            // silently stops error messages from tearing down a call that is
            // already ending.
            if (data.type === "offer") {
                send(socket, { type: "error", message: "Receiver is offline" });
            }
            return;
        }

        console.log("Forwarding", data.type, socket.userId, "->", data.to);
        send(receiver, { ...data, from: socket.userId });
    });

    socket.on("close", (code, reason) => {
        const id = socket.userId;

        // Only clear the map entry if this socket is the current one for that user
        if (id && clients.get(id) === socket) {
            clients.delete(id);
            userLastSeen[id] = Date.now();

            // ✅ Tell everyone this user went offline
            broadcastPresence(id, false, userLastSeen[id]);
        }

        console.log("Disconnected:", id, "Code:", code, "Reason:", reason.toString());
    });

    socket.on("error", (err) => console.error("Socket error:", err.message));
});

// ─────────────────────────────────────────────────────────────────────
// Heartbeat — detect dead connections (Wi-Fi drop, app killed)
// ─────────────────────────────────────────────────────────────────────
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 15000);

console.log("WebSocket server running on port 8080");