var newRedisClient = require('./redisutils').newRedisClient;


/**
 * @constructor
 * @param {String} setName
 * @param {redis.RedisClient} redisClient
 */
function WorkSet(setName, redisOpts) {
  var client = newRedisClient(redisOpts);
  var s = setName;

  /**
   * Queues are implemented as redis lists
   * @param {WorkItem} item data
   * @param {Function} callback
   */

  this.addToSet = function(item, callback){
    client.hmset(s, item.id, JSON.stringify(item), callback);
  }

  /**
   * Queues are implemented as redis lists
   * @param {String}   id
   * @param {Function} callback with the removed item value
   */
  this.removeFromSet = function(id, callback){
    client.hget(s, id, function(err, workItemStr){
      if (!err){
        client.hdel(s, id, function(err, result){
          callback(null, JSON.parse(workItemStr))
        });
      } else {
        // redis err
        callback(err);
      }
    });
  }
  /**
   * @return {number} number of members
   */
  this.setCount = function(callback){
    client.hlen(s,callback)
  }

  /**
   * Empties the set of all items
   */
  this.empty = function(callback){
    client.del(s, callback);
  }
}

exports.WorkSet = WorkSet;
