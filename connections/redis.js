const Redis = require("ioredis");

const config = require("../config");

class RedisConnection {

  constructor() {
    this.redis = null;
  }

  connect() {

    if (!this.redis) {

      this.redis = new Redis({
        sentinels: config.redis.sentinels,

        name: config.redis.name,

        db: config.redis.db,

        password: config.redis.password,

        retryStrategy(times) {
          console.log(`Redis retry attempt: ${times}`);

          return Math.min(times * 100, 3000);
        },
      });

      // -----------------------------------
      // EVENTS
      // -----------------------------------

      this.redis.on("connect", () => {
        console.log("Redis connected");
      });

      this.redis.on("ready", () => {
        console.log("Redis ready");
      });

      this.redis.on("error", (err) => {
        console.error("Redis error:", err);
      });

      this.redis.on("close", () => {
        console.log("Redis connection closed");
      });

      this.redis.on("reconnecting", () => {
        console.log("Redis reconnecting...");
      });
    }

    return this.redis;
  }

  getClient() {

    if (!this.redis) {
      return this.connect();
    }

    return this.redis;
  }
}

module.exports = new RedisConnection();