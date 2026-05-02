const express = require("express");
const bodyParser = require("body-parser");
const { Kafka } = require("kafkajs");
const path = require("path");
const { Pool } = require('pg');
const jwt = require("jsonwebtoken");
const bcrypt = require("bcrypt");
const http = require("http");
const socketIo = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

const PORT = 3000;
const SECRET_KEY = "my-secret-key-is-this-at-least-256-bits";
const SALT_ROUNDS = 10;

app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public')));

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'pagila',
    password: '123qwe',
    port: 5432,
});

const kafka = new Kafka({
    clientId: "ml-service",
    brokers: ["localhost:9092"]
});

const producer = kafka.producer();
const predconsumer = kafka.consumer({ groupId: "ml-data-group" });

io.on('connection', (socket) => {
    console.log('A user connected:', socket.id);
    socket.on('disconnect', () => {
        console.log('User disconnected:', socket.id);
    });
});

app.post("/api/ml-model/send", async (req, res) => {
    const loanFields = {
        income: req.body.income,
        employment_status: req.body.employment_status,
        credit_score: req.body.credit_score,
        loan_amount: req.body.loan_amount
    };

    if (Object.values(loanFields).every(val => val !== undefined && val !== null)) {
        if (
            loanFields.income <= 0 || loanFields.income > 200000 ||
            loanFields.credit_score < 300 || loanFields.credit_score > 850 ||
            loanFields.loan_amount <= 0
        ) {
            return res.status(400).json({
                error: "Invalid input: Income (0-200,000), Credit Score (300-850), Loan Amount (>0)."
            });
        }

        try {
            await producer.connect();
            await producer.send({
                topic: "dss-ml-model-input",
                messages: [{ key: Date.now().toString(), value: JSON.stringify(loanFields) }]
            });
            await producer.disconnect();
            res.status(201).json({ status: "Sent to Kafka", ...loanFields });
        } catch (err) {
            console.error("Kafka send failed:", err.message);
            res.status(500).json({ error: "Kafka send failed", details: err.message });
        }
    } else {
        res.status(400).json({
            error: "Invalid input: Provide all fields (income, employment_status, credit_score, loan_amount)."
        });
    }
});

const startKafkaPredictionListener = async () => {
    await predconsumer.connect();
    await predconsumer.subscribe({ topic: "dss-ml-model-output", fromBeginning: true });
    await predconsumer.subscribe({ topic: "dss-ml-model-output-logs", fromBeginning: true });

    await predconsumer.run({
        eachMessage: async ({ topic, message }) => {
            if (topic === "dss-ml-model-output") {
                try {
                    const modelResult = JSON.parse(message.value.toString());
                    console.log("Received ML output from Kafka:", modelResult);
                    io.emit('model-result', modelResult);
                } catch (err) {
                    console.error("Error parsing Kafka message:", err.message);
                }
            } else if (topic === "dss-ml-model-output-logs") {
                const log_result = JSON.parse(message.value.toString());
                if (log_result.income !== undefined) {
                    await pool.query(
                        `INSERT INTO logs_pred_loan (income, employment_status, credit_score, loan_amount, approved, timestamp)
                         VALUES ($1, $2, $3, $4, $5, $6)`,
                        [
                            log_result.income,
                            log_result.employment_status,
                            log_result.credit_score,
                            log_result.loan_amount,
                            log_result.approved,
                            log_result.timestamp
                        ]
                    );
                }
            }
        }
    });
};
startKafkaPredictionListener().catch(console.error);

app.get('/api/logs-pred', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT approved, COUNT(*) AS counts_f FROM logs_pred_loan GROUP BY approved ORDER BY counts_f DESC'
        );
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: 'Database error' });
    }
});

app.get('/api/logs-pred_scatter', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT credit_score, loan_amount, approved FROM logs_pred_loan'
        );
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: 'Database error' });
    }
});

app.get('/api/predictions', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT income, employment_status, credit_score, loan_amount, approved, timestamp FROM logs_pred_loan'
        );
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: 'Database error' });
    }
});

const authenticateToken = (req, res, next) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
        console.error("Missing or invalid Authorization header:", authHeader);
        return res.status(401).json({ error: "Unauthorized: Missing or invalid token" });
    }
    const token = authHeader.split(" ")[1];
    try {
        const decoded = jwt.verify(token, SECRET_KEY);
        req.decodedToken = decoded;
        console.log("Token verified successfully for user:", decoded.username);
        next();
    } catch (error) {
        console.error("Token verification error:", error.message);
        return res.status(403).json({ error: "Forbidden: Invalid token" });
    }
};

app.post("/register", async (req, res) => {
    const { username, password, firstname, lastname, role } = req.body;
    try {
        const hashedPassword = await bcrypt.hash(password, SALT_ROUNDS);
        await pool.query(
            "INSERT INTO users (username, password, firstname, lastname, role) VALUES ($1, $2, $3, $4, $5)",
            [username, hashedPassword, firstname, lastname, role]
        );
        res.json({ message: "User registered successfully!" });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post("/login", async (req, res) => {
    const { username, password } = req.body;
    try {
        const result = await pool.query("SELECT * FROM users WHERE username = $1", [username]);
        const user = result.rows[0];
        if (!user || !(await bcrypt.compare(password, user.password))) {
            return res.status(401).json({ error: "Invalid credentials" });
        }
        const token = jwt.sign(
            { id: user.id, username: user.username, firstname: user.firstname, lastname: user.lastname, role: user.role },
            SECRET_KEY,
            { expiresIn: "30d" }
        );
        res.json({ token });
    } catch (err) {
        res.status(500).json({ error: "Server error" });
    }
});

server.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
});
