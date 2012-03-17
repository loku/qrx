var redis = require('redis'),
    uuid = require('node-uuid'),
    Rx = require('rx').Rx,
    WorkItem = require('./workitem').WorkItem,
    newRedisClient = require('./redisutils').newRedisClient;

/**
 * Light weight work queue wrapper for redis queue
 * based on redis list, all redis queue operations are O(1)
 * WorkQueue instances have their own connections due to due
 * potentially connection blocking operations
 * blocking operations intance a new connection to NOT block
 * other work queue operations
 *
 * @contructor
 * @param {String} qname
 * @param {Objsect} redisOpts
 */
function WorkQueue(qname, redisOpts) {



  // WorkQueue has uses a shared client connection for NON-BLOCKING
  // operations
  var client = newRedisClient(redisOpts);

  // q's are mapped to redis keys by name
  this.q = qname;

  /**
   * Enqueues a WorkItem, wrapping item object in WorkItem if not WorkItem
   * @param {Object|WorkItem}   item
   * @param {Function} callback
   */
  this.enqueue = function(item, callback){
   // items LPUSH as redis q's sensed leftwards

   // if already WorkItem, no need to pack in WorkItem
   var workItem = null;
   if (item instanceof WorkItem){
      workItem = item
   } else {
      workItem = new WorkItem(item);
   }

   client.lpush(this.q, JSON.stringify(workItem), function(err, length){
      if (!err) {
         callback(null, {id:workItem.id, queueLength:length});
      } else {
         // call back with ths redis err
         callback(err);
      }
   });
  }


  /**
   * Dequeues FIFO from the WorkQueue
   * @param {function} callback with head WorkItem or null
   */
  this.dequeue = function(callback){
    client.rpop(this.q, function(err, workItemStr){
      if (!err){
         callback(null, JSON.parse(workItemStr));
      } else {
         callback(err);
      }
   });
  }

  /**
   * Peeks the first item in the FIFO
   * @param {function} callback
   * @return {WorkItem}
   */
  this.peek = function(callback){
    client.lindex(this.q, 0, function(err, workItemStr){
      if (!err){
        callback(null, JSON.parse(workItemStr));
      } else {
        callback(err);
      }
    })
  }

  /**
   * Blocks the callback waiting for a new item in the queue
   * no other operations are possible on the queue when blocked
   * @param {redis.RedisClient} client
   * @param {Integer} timeout in miliseconds or 0 if infinite
   * @param {function} callback
   */
  this.blockingDequeue = function(client, timeout, callback){
    // blocks the client
    client.brpop(this.q, timeout, function(err, workItemStr){
      if (!err){
        callback(null, JSON.parse(workItemStr[1]));
      } else {
         callback(err);
      }
    });
  }

  /**
   * Transactionally block and dequeues an item from one queue
   * and enqueues workitem on the newq transactionally
   * @param {redis.RedisClient} client
   * @param {String} nextWorkQueueName
   * @param {Integer} timeout
   * @param {function} callback
   *
   */
  this.txnBlockingDequeueEnqueue = function(client, nextQueueName, timeout, callback){
    // blocks the connection
    client.brpoplpush(this.q, nextQueueName, timeout, callback);
  }


  /**
   * Returns the length of the WorkQueue
   * @param {Function} callback with count or err
   */
  this.length = function(callback){
    client.llen(this.q, callback);
  }

  /**
   * Empties the queue of all items
   */
  this.empty = function(callback){
    client.del(this.q, callback);
  }




  /**
   * Returns an observable to the head of a q or Empty
   * @return {Rx.Observable} of WorkItem(s)
   */
  this.dequeueRx = function() {
    var self = this;
    return Rx.Observable.Create(function(obs){
     self.dequeue(function(err, item){
        if (!err){
          if (item){
            obs.OnNext(item);
          }
          // complete with no next is empty
          obs.OnCompleted();
        } else {
          obs.OnError(err);
        }
      });
      return function(){};
   });
  }

  /**
   * Returns an observable of the head blocking the
   * connection until a head is available or timeout
   * @param {redis.RedisClient} client
   * @param {Integer} [timeout] in ms, infinite if not spec'd
   */
  this.blockingDequeueRx = function(timeout){
    var self = this;
    var rc = newRedisClient(redisOpts);
    // create a new connection to block for the
    // the result
    return Rx.Observable.Create(function(obs){
      self.blockingDequeue(rc, timeout || 0, function(err, result){
        if (!err && result){
          obs.OnNext(result);
          obs.OnCompleted();
        } else {
          obs.OnError(err);
        }
      });
      return function(){
       // kill the redis connection, which breaks the block
       rc.quit();
      };
    });
  }

  /**
   * Infinite drain of WorkQueue, which only completes, when timeout is
   * exceeeded
   * blocking WorkQueue connection
   * @param {Integer} [timeout] in ms for block or null if infinite
   * @param {Integer} [throttle] in ms to space dequeues over time
   * return {Rx.Observable}
   */
  this.blockingDrainRx = function(timeout, throttle) {
    // create new connection bound into the Observable
    var self = this;
    var rc = newRedisClient(redisOpts);
    return Rx.Observable.Create(function(obs){
      var intervalSubs = null;
      intervalSubs = Rx.Observable.Interval(throttle || 0).Subscribe(function(_){
        self.blockingDequeue(rc, timeout || 0, function(err, result){
          if (result){
            obs.OnNext(result);
          } else if (!err) {
            obs.OnCompleted();
          } else {
            obs.OnError(err);
          }
        });
      });
      return function(){
        rc.quit();
        intervalSubs.Dispose();
      };
    });
  }

}

exports.WorkQueue = WorkQueue;