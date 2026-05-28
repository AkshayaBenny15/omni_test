const { Kafka, CompressionTypes } = require("kafkajs");
const config = require("../config");
// const logger = require("../log/logger");

class KafkaMessaging {
  constructor() {
    this.kafka = null;
    this.producer = null;
    this.consumer = null;

    this.isProducerConnected = false;
    this.isConsumerConnected = false;

    // Cache for topic existence (TTL: 1 hour = 3600000ms)
    this.topicCache = new Map();
    this.topicCacheTTL = 3600000;
  }

  // 🔹 Initialize Kafka client (common)
  initKafka() {
    if (!this.kafka) {
      console.log("config.kafka.brokers ", config.kafka.brokers);

      this.kafka = new (require("kafkajs").Kafka)({
        clientId: config.kafka.clientId,
        brokers: config.kafka.brokers,
        retry: {
          initialRetryTime: 100,
          retries: 2,
        },
      });
    }
  }

  // 🔹 Producer init
  async initProducer() {
    try {
      this.initKafka();

      if (!this.producer) {
        this.producer = this.kafka.producer({
          maxInFlightRequests: 1,
          idempotent: true,
          transactionTimeout: 30000,
        });
      }

      if (!this.isProducerConnected) {
        await this.producer.connect();
        this.isProducerConnected = true;
        // logger.info("Kafka Producer connected successfully");
      }

      return this.producer;
    } catch (error) {
      // logger.error("Kafka Producer initialization failed:", error);
      throw error;
    }
  }

  // 🔹 Consumer init
  async initConsumer() {
    try {
      this.initKafka();

      if (!this.consumer) {
        this.consumer = this.kafka.consumer({
          groupId: config.kafka.groupId,
          sessionTimeout: 30000,
          heartbeatInterval: 3000,
          maxWaitTimeInMs: 5000,
        });
      }

      if (!this.isConsumerConnected) {
        await this.consumer.connect();
        this.isConsumerConnected = true;
        // logger.info("Kafka Consumer connected successfully");
      }

      return this.consumer;
    } catch (error) {
      // logger.error("Kafka Consumer initialization failed:", error);
      throw error;
    }
  }

  // 🔹 Check if topic exists (with caching)
  async topicExists(topic) {
    try {
      // Check cache first
      if (this.topicCache.has(topic)) {
        const cached = this.topicCache.get(topic);
        if (Date.now() - cached.timestamp < this.topicCacheTTL) {
          return cached.exists;
        }
        this.topicCache.delete(topic); // Expired cache entry
      }

      // this.initKafka();
      const admin = this.kafka.admin();
      await admin.connect();

      try {
        const topics = await admin.fetchTopicMetadata({ topics: [topic] });
        const exists =
          topics.topics.length > 0 && topics.topics[0].name === topic;
        await admin.disconnect();

        // Cache the result
        this.topicCache.set(topic, { exists, timestamp: Date.now() });
        return exists;
      } catch (error) {
        await admin.disconnect();
        // Topic doesn't exist
        this.topicCache.set(topic, { exists: false, timestamp: Date.now() });
        return false;
      }
    } catch (error) {
      // logger.error(`Error checking topic ${topic}:`, error);
      return false;
    }
  }

  // 🔹 Create topic asynchronously (background, non-blocking)
  async createTopicAsync(topic, numPartitions = 1, replicationFactor = 1) {
    try {
      //this.initKafka();
      const admin = this.kafka.admin();
      await admin.connect();

      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions,
            replicationFactor,
          },
        ],
        waitForLeaders: true,
        validateOnly: false,
      });

      await admin.disconnect();

      // Update cache
      this.topicCache.set(topic, { exists: true, timestamp: Date.now() });
      // logger.info(`Topic ${topic} created`);

      return true;
    } catch (error) {
      // logger.error(`Error creating topic ${topic}:`, error);
      // console.log(`Error creating topic ${topic}:`, error);
      return false;
    }
  }

  // 🔹 Check topic exists, create if needed (async), then publish message (non-blocking topic creation)
  async publishWithTopicCheck(topic, message, numPartitions = 3) {
    try {
      // Check if topic exists (uses cache)
      const exists = await this.topicExists(topic);

      // Create topic in background if it doesn't exist (fire-and-forget, non-blocking)
      if (!exists) {
        // logger.info(
        //   `Topic ${topic} needs creation. Scheduling async creation...`,
        // );
        // Fire-and-forget: don't await, don't block
        await this.createTopicAsync(topic).catch((err) => {
          // logger.error(`Background topic creation failed for ${topic}:`, err);
        });
      }

      // Publish message immediately - key determines partition (e.g., "US-IN" will hash to specific partition)
      // If topic doesn't exist yet, Kafka will create it automatically or fail gracefully
      const result = this.publishMessage(topic, message);
      return result;
    } catch (error) {
      // logger.error(`Error in publishWithTopicCheck for ${topic}:`, error);
      throw error;
    }
  }

  // 🔹 Publish message (ensure producer is initialized)
  async publishMessage(topic, message, key = null) {
    try {
      await this.initProducer();

      const result = await this.producer.send({
        topic,
        messages: [
          {
            key: key || null,
            value: JSON.stringify(message),
            timestamp: Date.now(),
          },
        ],
        compression: CompressionTypes.GZIP,
      });

      // logger.info(
      //   `Message published to topic: ${topic}, partition: ${result[0].partition}`,
      //   { result },
      // );
      return result;
    } catch (error) {
      // logger.error("Kafka publish error:", error);
      throw error;
    }
  }

  async subscribeToTopics(topics, messageHandler) {
    if (!this.isConnected) throw new Error("Kafka not connected");

    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const parsedMessage = JSON.parse(message.value.toString());
          await messageHandler(topic, parsedMessage, {
            partition,
            offset: message.offset,
            timestamp: message.timestamp,
            key: message.key ? message.key.toString() : null,
          });
        } catch (error) {
          // logger.error("Message processing error:", error);
        }
      },
    });

    // logger.info(`Subscribed to topics: ${topics.join(", ")}`);
  }

  // ...existing code...

  // 🔹 Assign specific partitions to consumer (manual assignment)
  // 🔹 Subscribe to topic(s) for consumption instead of manual partition assignment
  async assignPartitions(assignments) {
    try {
      await this.initConsumer();

      if (this.isConsumerRunning) {
        // logger.info(
        //   "Stopping running consumer before subscribing to new topics",
        // );
        console.log(
          "Stopping running consumer before subscribing to new topics",
        );
        try {
          await this.consumer.stop();
        } catch (stopError) {
          // logger.warn(
          //   "Failed to stop running consumer before assigning partitions",
          //   stopError,
          // );
          console.log(
            "Failed to stop running consumer before assigning partitions",
          );
        }
        this.isConsumerRunning = false;
      }

      for (const { topic } of assignments) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }
      // logger.info(
      //   `Subscribed to assigned topics: ${JSON.stringify(assignments)}`,
      // );
      console.log(
        `Subscribed to assigned topics: ${JSON.stringify(assignments)}`,
      );
    } catch (error) {
      // logger.error("Error assigning partitions:", error);
      console.error("Error assigning partitions:", error);
      throw error;
    }
  }

  // ...existing code...

  async close() {
    try {
      if (this.producer) {
        await this.producer.disconnect();
      }
      if (this.consumer) {
        await this.consumer.disconnect();
      }
      this.isConnected = false;
      // logger.info("Kafka connections closed");
    } catch (error) {
      // logger.error("Kafka close error:", error);
    }
  }
}

module.exports = new KafkaMessaging();
