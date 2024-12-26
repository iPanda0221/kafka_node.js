const Fastify = require("fastify");
const { Kafka } = require("kafkajs");

const fastify = Fastify({ logger: true });

// Kafka Producer Setup
const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

fastify.post("/order", async (request, reply) => {
  const { orderId, items, amount } = request.body;

  // Produce an 'OrderCreated' event to Kafka
  await producer.connect();
  await producer.send({
    topic: "order-created",
    messages: [
      {
        key: String(orderId),
        value: JSON.stringfy({ orderId, items, amount }),
      },
    ],
  });

  reply.send({ status: "Order created", orderId });
});

//Start Fastify Server
fastify.listen({ port: 3001 }, (err, address) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  fastify.log.info(`Order Service is running on ${address}`);
});
