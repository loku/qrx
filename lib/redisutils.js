var redis = require('redis');

/**
 * Returns a new redis client conditionally on having options
 * @return {redis.RedisClient} new client based on supplied options
 */
function newRedisClient(redisOpts) {
  return redisOpts ?
          redis.createClient(redisOpts.port, redisOpts.host, redisOpts):
          redis.createClient();
}

exports.newRedisClient = newRedisClient;