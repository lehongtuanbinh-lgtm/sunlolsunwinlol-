const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const os = require('os');
const fs = require('fs'); 
const path = require('path'); 

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3001;

// --- HỆ THỐNG LƯU TRỮ LỊCH SỬ AN TOÀN ---
const HISTORY_FILE = path.join(__dirname, 'history.json');

// Hàm tải lịch sử từ file (chạy khi khởi động server)
function loadHistory() {
    try {
        if (fs.existsSync(HISTORY_FILE)) {
            const data = fs.readFileSync(HISTORY_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (err) {
        console.error('[❌] Lỗi đọc file lịch sử:', err.message);
    }
    return []; // Trả về mảng rỗng nếu chưa có file
}

// Hàm lưu lịch sử an toàn (Tránh mất dữ liệu khi crash ngang)
function saveHistory(historyArray) {
    try {
        // Ghi vào file tạm trước, sau đó đổi tên để tránh lỗi hỏng file (corrupt) khi server sập
        const tempFile = HISTORY_FILE + '.tmp';
        fs.writeFileSync(tempFile, JSON.stringify(historyArray, null, 4), 'utf8');
        fs.renameSync(tempFile, HISTORY_FILE);
    } catch (err) {
        console.error('[❌] Lỗi lưu file lịch sử:', err.message);
    }
}

// Khởi tạo biến lưu trữ toàn bộ lịch sử trong RAM
let sessionHistory = loadHistory();
// ---------------------------------

let apiResponseData = {
    "Phien": null,
    "Xuc_xac_1": null,
    "Xuc_xac_2": null,
    "Xuc_xac_3": null,
    "Tong": null,
    "Ket_qua": "",
    "id": "by hoàng",
    "contact": "@hoangvip247",
    "server_time": new Date().toISOString()
};

let currentSessionId = null;

const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.vin"
};
const RECONNECT_DELAY = 2500;
const PING_INTERVAL = 15000;

const initialMessages = [
    [
        1,
        "MiniGame",
        "GM_apivopnhaan",
        "WangLin",
        {
            "info": "{\"ipAddress\":\"113.185.45.88\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJwbGFtYW1hIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6MzMxNDgxMTYyLCJhZmZJZCI6IkdFTVdJTiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoiZ2VtIiwidGltZXN0YW1wIjoxNzY2NDc0NzgwMDA2LCJsb2NrR2FtZXMiOltdLCJhbW91bnQiOjAsImxvY2tDaGF0IjpmYWxzZSwicGhvbmVWZXJpZmllZCI6ZmFsc2UsImlwQWRkcmVzcyI6IjExMy4xODUuNDUuODgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzE4LnBuZyIsInBsYXRmb3JtSWQiOjUsInVzZXJJZCI6IjZhOGI0ZDM4LTFlYzEtNDUxYi1hYTA1LWYyZDkwYWFhNGM1MCIsInJlZ1RpbWUiOjE3NjY0NzQ3NTEzOTEsInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fYXBpdm9wbmhhYW4ifQ.YFOscbeojWNlRo7490BtlzkDGYmwVpnlgOoh04oCJy4\",\"locale\":\"vi\",\"userId\":\"6a8b4d38-1ec1-451b-aa05-f2d90aaa4c50\",\"username\":\"GM_apivopnhaan\",\"timestamp\":1766474780007,\"refreshToken\":\"63d5c9be0c494b74b53ba150d69039fd.7592f06d63974473b4aaa1ea849b2940\"}",
            "signature": "66772A1641AA8B18BD99207CE448EA00ECA6D8A4D457C1FF13AB092C22C8DECF0C0014971639A0FBA9984701A91FCCBE3056ABC1BE1541D1C198AA18AF3C45595AF6601F8B048947ADF8F48A9E3E074162F9BA3E6C0F7543D38BD54FD4C0A2C56D19716CC5353BBC73D12C3A92F78C833F4EFFDC4AB99E55C77AD2CDFA91E296"
        }
    ],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let connectionTimeout = null; // Biến chống kẹt kết nối (zombie connection)

// Xử lý chống crash app khi có lỗi ngầm
process.on('uncaughtException', (err) => {
    console.error('[CRITICAL] Uncaught Exception:', err);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error('[CRITICAL] Unhandled Rejection at:', promise, 'reason:', reason);
});

const getNetworkInfo = () => {
    const interfaces = os.networkInterfaces();
    let localIP = '127.0.0.1';
    
    for (const ifaceName in interfaces) {
        for (const iface of interfaces[ifaceName]) {
            if (!iface.internal && iface.family === 'IPv4') {
                localIP = iface.address;
                break;
            }
        }
    }
    return { localIP };
};

// Hàm reset heartbeat để không bị rớt gói tin
function heartbeat() {
    clearTimeout(connectionTimeout);
    connectionTimeout = setTimeout(() => {
        if (ws) {
            console.log('[⚠️] Kết nối bị treo ngầm, đang ép đóng để reconnect...');
            ws.terminate();
        }
    }, PING_INTERVAL + 5000);
}

function connectWebSocket() {
    if (ws) {
        ws.removeAllListeners();
        ws.terminate(); 
    }

    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS });

    ws.on('open', () => {
        console.log('[✅] WebSocket connected to Sun.Win');
        heartbeat(); // Kích hoạt chống kẹt
        
        initialMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify(msg));
                }
            }, i * 600);
        });

        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.ping();
            }
        }, PING_INTERVAL);
    });

    ws.on('ping', heartbeat);
    ws.on('pong', heartbeat);

    ws.on('message', (message) => {
        heartbeat(); // Reset timeout nếu nhận đc bất kỳ msg nào
        try {
            const data = JSON.parse(message);

            if (!Array.isArray(data) || typeof data[1] !== 'object') {
                return;
            }

            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1008 && sid) {
                currentSessionId = sid;
                console.log(`[🎮] Đang chờ kết quả phiên: ${sid}`);
            }

            if (cmd === 1003 && gBB) {
                if (!d1 || !d2 || !d3) return;

                const total = d1 + d2 + d3;
                const result = (total > 10) ? "Tài" : "Xỉu";

                // Xử lý trường hợp lỡ mất tín hiệu 1008 (thường do server lag/reconnect)
                let phienLuu = currentSessionId;
                if (!phienLuu && sessionHistory.length > 0) {
                    phienLuu = sessionHistory[0].Phien + 1; // Nội suy ID phiên dựa trên lịch sử
                }

                // Chặn lưu trùng lặp 1 phiên nhiều lần (Fix lỗi lặp data)
                if (sessionHistory.length > 0 && sessionHistory[0].Phien === phienLuu) {
                    return; 
                }

                // Cập nhật API kết quả hiện tại
                apiResponseData = {
                    "Phien": phienLuu,
                    "Xuc_xac_1": d1,
                    "Xuc_xac_2": d2,
                    "Xuc_xac_3": d3,
                    "Tong": total,
                    "Ket_qua": result,
                    "id": "by hoàng",
                    "contact": "@hoangvip247",
                    "server_time": new Date().toISOString(),
                    "update_count": (apiResponseData.update_count || 0) + 1
                };
                
                // THÊM VÀO LỊCH SỬ VÀ LƯU FILE LUÔN
                const historyEntry = {
                    "Phien": phienLuu,
                    "Xuc_xac_1": d1,
                    "Xuc_xac_2": d2,
                    "Xuc_xac_3": d3,
                    "Tong": total,
                    "Ket_qua": result,
                    "Time": Date.now() // Sử dụng ms timestamp chuẩn
                };
                
                // unshift: Đẩy phiên mới nhất lên đầu mảng (index 0) 
                sessionHistory.unshift(historyEntry);
                
                // Ghi ngay vào file history.json một cách an toàn
                saveHistory(sessionHistory); 
                
                console.log(`[🎲] Đã lưu Phiên ${phienLuu}: ${d1}-${d2}-${d3} = ${total} (${result})`);
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý message:', e.message);
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`[🔌] WebSocket closed. Code: ${code}`);
        clearInterval(pingInterval);
        clearTimeout(connectionTimeout);
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        console.error('[❌] WebSocket error:', err.message);
        ws.terminate();
    });
}

// ROUTES PUBLIC
app.get('/api/ditmemaysun', (req, res) => {
    res.json(apiResponseData);
});

// API LẤY TOÀN BỘ LỊCH SỬ CHUẨN FORM
app.get('/api/sun', (req, res) => {
    res.json({
        currentSessionId: currentSessionId || (sessionHistory.length > 0 ? sessionHistory[0].Phien + 1 : null),
        total: sessionHistory.length,
        data: sessionHistory
    });
});

app.get('/api/history', (req, res) => {
    res.json({
        current: apiResponseData,
        message: "Chế độ VIP: Vui lòng truy cập /api/sun để xem toàn bộ lịch sử các phiên"
    });
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        websocket: ws ? ws.readyState === WebSocket.OPEN : false,
        uptime: process.uptime(),
        connections: ws ? 'connected' : 'disconnected'
    });
});

app.get('/', (req, res) => {
    const html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>by hoàng AI Analysis</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; 
                padding: 20px; 
                background: #050b14; 
                color: #ffffff; 
                display: flex;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
            }
            .glass-panel { 
                background: rgba(0, 30, 60, 0.4); 
                backdrop-filter: blur(12px); 
                -webkit-backdrop-filter: blur(12px);
                border: 1px solid rgba(0, 255, 255, 0.2); 
                border-radius: 16px; 
                padding: 40px; 
                box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
                text-align: center;
                width: 100%;
                max-width: 600px;
            }
            .flash-animation {
                animation: strobe 0.8s infinite alternate;
                font-size: 3em;
                font-weight: 800;
                margin: 20px 0;
            }
            .tai { color: #00ffff; }
            .xiu { color: #ff4500; }
            
            @keyframes strobe {
                0% { text-shadow: 0 0 10px currentColor, 0 0 20px currentColor; opacity: 1; }
                100% { text-shadow: 0 0 5px currentColor; opacity: 0.7; }
            }
            .info-text { color: #a0aec0; font-size: 1.1em; margin: 5px 0; }
            .badge {
                display: inline-block;
                padding: 5px 15px;
                border-radius: 20px;
                background: rgba(0, 255, 255, 0.1);
                border: 1px solid rgba(0, 255, 255, 0.3);
                color: #00ffff;
                margin-bottom: 20px;
            }
        </style>
    </head>
    <body>
        <div class="glass-panel">
            <div class="badge">by hoàng Real-Time Analysis</div>
            
            <div id="result-display" class="flash-animation ${apiResponseData.Ket_qua === 'Tài' ? 'tai' : 'xiu'}">
                ${apiResponseData.Tong ? `${apiResponseData.Xuc_xac_1}-${apiResponseData.Xuc_xac_2}-${apiResponseData.Xuc_xac_3} = ${apiResponseData.Tong} (${apiResponseData.Ket_qua})` : 'ĐANG PHÂN TÍCH...'}
            </div>
            
            <p class="info-text" id="session-info">Phiên: ${apiResponseData.Phien || 'N/A'}</p>
            <p class="info-text">Trạng thái: <span style="color: #00ff00;">Live Connection</span></p>
            <p class="info-text" style="font-size: 0.9em; margin-top: 15px;"><a href="/api/sun" style="color: #00ffff; text-decoration: none;">[Xem Toàn Bộ Lịch Sử]</a></p>
        </div>
        
        <script>
            setInterval(() => {
                fetch('/api/ditmemaysun')
                    .then(res => res.json())
                    .then(data => {
                        if(data.Tong) {
                            const resultDiv = document.getElementById('result-display');
                            const sessionDiv = document.getElementById('session-info');
                            
                            resultDiv.textContent = \`\${data.Xuc_xac_1}-\${data.Xuc_xac_2}-\${data.Xuc_xac_3} = \${data.Tong} (\${data.Ket_qua})\`;
                            resultDiv.className = \`flash-animation \${data.Ket_qua === 'Tài' ? 'tai' : 'xiu'}\`;
                            sessionDiv.textContent = 'Phiên: ' + data.Phien;
                        }
                    })
                    .catch(e => console.log('Chờ kết nối...'));
            }, 1000);
        </script>
    </body>
    </html>
    `;
    
    res.send(html);
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n=========================================`);
    console.log(`🚀 HOÀNG ELITE DATA STREAM`);
    console.log(`=========================================`);
    console.log(`📡 Port: ${PORT}`);
    console.log(`🔌 Connecting to WebSocket...`);
    console.log(`=========================================\n`);
    
    connectWebSocket();
});