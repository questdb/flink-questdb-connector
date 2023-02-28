const WebSocket = require('ws');
const { Kafka, logLevel} = require('kafkajs');

const kafka = new Kafka({
    clientId: 'producer',
    brokers: ['kafka:9092'],
    logLevel: logLevel.INFO,
    retry: {
        initialRetryTime: 1000,
        retries: 10,
    }
});

const producer = kafka.producer();

async function ingest() {
    try {
        await producer.connect();
        const ws = new WebSocket('wss://ws-feed.pro.coinbase.com');
        await new Promise(resolve => ws.once('open', resolve));

        ws.send(JSON.stringify({
            "type": "subscribe",
            "product_ids": [
                "BTC-USD"
            ],
            "channels": [
                "level2",
                "heartbeat",
                {
                    "name": "ticker",
                    "product_ids": [
                        "BTC-USD"
                    ]
                }
            ]
        }));

        ws.on('message', async msg => {
            await producer.send({
                topic: "Orders",
                messages: [
                    {
                        value: msg
                    }
                ]
            });
        });

    } catch(err){
        console.error(err);
    }
}

ingest();