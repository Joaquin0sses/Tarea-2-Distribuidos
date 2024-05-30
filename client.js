const { Kafka } = require('kafkajs');

// Configure Kafka client
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'] // Replace with your Kafka broker address
});

// Create a producer
const producer = kafka.producer();

// Create a consumer
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
    // Connect the producer
    await producer.connect();
    console.log('Producer connected');

    // Send a test message
    await producer.send({
        topic: 'test-topic',
        messages: [
            { value: 'Hello KafkaJS user!' }
        ]
    });
    console.log('Message sent');

    // Connect the consumer
    await consumer.connect();
    console.log('Consumer connected');

    // Subscribe to the topic
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

    // Consume messages
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            });
        },
    });
};

run().catch(e => console.error(`[example/producer] ${e.message}`, e));

// Disconnect the producer and consumer gracefully on exit
process.on('SIGINT', async () => {
    await producer.disconnect();
    await consumer.disconnect();
    process.exit(0);
});
