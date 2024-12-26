const Fastify = require("fastify");
const { Kafka } = require("kafkajs");

const fastify = Fastify({ logger: true });

// Kafka Setup
const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "inventory-group" });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "order-created", fromBeginning: true });

  // Consume messages from 'order-created' topic
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      fastify.log.info(`Processing order ${order.orderId}`);

      // Simulate inventory check
      const inventoryChecked = {
        orderId: order.orderId,
        inventoryStatus: "available",
      };

      // Produce 'InventoryChecked' event to Kafka
      await producer.connect();
      await producer.send({
        topic: "inventory-checked",
        messages: [
          {
            key: String(order.orderId),
            value: JSON.stringify(inventoryChecked),
          },
        ],
      });
    },
  });
};

// Start Fastify Server and Kafka Consumer
fastify.listen({ port: 3002 }, (err, address) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  fastify.log.info(`Inventory Service is running on ${address}`);
  run().catch(console.error);
});
