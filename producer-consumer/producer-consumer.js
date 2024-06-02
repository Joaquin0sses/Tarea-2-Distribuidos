import { Kafka } from "kafkajs";
import express from "express";

const kafka = new Kafka({
    clientId: "producer-consumer",
    brokers: ["kafka:9092"],
});

// Producer setup
const producer = kafka.producer();

// Consumer setup
const consumer = kafka.consumer({ groupId: "consumer-group" });
let values = [];

const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "calc-stats", fromBeginning: true });

    await consumer.run({
        eachMessage: async function({ message }) {
            const value = Number(message.value.toString());

            values.push(value);

            if (values.length < 100) return;

            console.log({
                min: Math.min(...values),
                max: Math.max(...values),
                avg: values.reduce((a, b) => a + b, 0) / values.length,
            });

            values = [];
        }
    });
};

runConsumer().catch(console.error);

// Express app for producer
const app = express();
const port = 3000;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.post("/", async (req, res) => {
    const value = req.body.value;

    if (isNaN(value)) {
        res.sendStatus(400);
    } else {
        await producer.connect();
        await producer.send({
            topic: "calc-stats",
            messages: [{ value: value }],
        });
        await producer.disconnect();

        res.sendStatus(200);
    }
});

app.listen(port, () => {
    console.log(`App listening on port ${port}`);
});
