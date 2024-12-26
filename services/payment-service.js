const Fastify = require("fastify");
const { Kafka } = require("kafkajs");

const fastify = Fastify({ logger: true });

// Kafka Consumer Setup
const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "payment-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "inventory-checked", fromBeginning: true });

  // Consume messages from 'inventory-checked' topic
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      fastify.log.info(
        `Inventory status for order ${event.orderId}: ${event.inventoryStatus}`
      );

      if (event.inventoryStatus === "available") {
        // Process payment
        fastify.log.info(`Processing payment for order ${event.orderId}`);
      }
    },
  });
};

// Start Fastify Server and Kafka Consumer
fastify.listen({ port: 3003 }, (err, address) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  fastify.log.info(`Payment Service is running on ${address}`);
  run().catch(console.error);
});
