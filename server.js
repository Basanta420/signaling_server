
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;

const clients = new Map(); // userId -> socket
const userLastSeen = {};   // userId -> epoch ms

// HTTP server for Render
const server = http.createServer((req, res) => {
    if (req.url === "/") {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("MyChat Signaling Server is running");
        return;
    }

    res.writeHead(404);
    res.end("Not Found");
});

// WebSocket server attached to HTTP server
const wss = new WebSocket.Server({ server });

// Helpers
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

// Connection
wss.on("connection", (socket, req) => {

    socket.isAlive = true;

    socket.on("pong", () => {
        socket.isAlive = true;
    });

    console.log("Client connected:", req.socket.remoteAddress);

    socket.on("message", (raw) => {

        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch (e) {
            console.error("Invalid JSON:", e.message);
            return;
        }

        // Register
        if (data.type === "register") {

            const id = String(data.userId || "").trim();

            if (!id) return;

            const old = clients.get(id);

            if (old && old !== socket) {
                old.userId = null;
                old.close(4000, "replaced by new connection");
            }

            socket.userId = id;
            clients.set(id, socket);

            userLastSeen[id] = Date.now();

            console.log("Registered:", id);

            send(socket, {
                type: "registered",
                userId: id
            });

            broadcastPresence(id, true, null);

            return;
        }

        // Require registration
        if (!socket.userId) {
            send(socket, {
                type: "error",
                message: "Not registered"
            });
            return;
        }

        // Subscribe to presence
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

        // Forward WebRTC signaling messages
        const receiver = clients.get(data.to);

        if (!receiver || receiver.readyState !== WebSocket.OPEN) {

            console.log("Receiver offline:", data.to, data.type);

            if (data.type === "offer") {
                send(socket, {
                    type: "error",
                    message: "Receiver is offline"
                });
            }

            return;
        }

        console.log(
            "Forwarding",
            data.type,
            socket.userId,
            "->",
            data.to
        );

        send(receiver, {
            ...data,
            from: socket.userId
        });
    });

    socket.on("close", (code, reason) => {

        const id = socket.userId;

        if (id && clients.get(id) === socket) {

            clients.delete(id);

            userLastSeen[id] = Date.now();

            broadcastPresence(id, false, userLastSeen[id]);
        }

        console.log(
            "Disconnected:",
            id,
            "Code:",
            code,
            "Reason:",
            reason.toString()
        );
    });

    socket.on("error", (err) => {
        console.error("Socket error:", err.message);
    });
});

// Heartbeat
setInterval(() => {

    wss.clients.forEach((ws) => {

        if (!ws.isAlive) {
            return ws.terminate();
        }

        ws.isAlive = false;
        ws.ping();
    });

}, 15000);

// Start server
server.listen(PORT, "0.0.0.0", () => {
    console.log(`MyChat signaling server running on port ${PORT}`);
});
