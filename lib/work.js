var redis = require('redis'),
    Rx = require('rx').Rx;
    uuid = require('node-uuid');
/**
 * @constructor
 * Primative work datum with guaranteed unique id
 * @param {Object} work
 */

// instance + sequence id

function WorkItem(work){
  this.id =  uuid.v1();
  
  this.work = work;
  // completed state
  this.status = null;
  this.completedWork = null;
  this.error = null;
}

function cloneCompleted(workItem, err, completedWork){
    var newWorkItem = new WorkItem(null);
    newWorkItem.id = workItem.id;
    newWorkItem.work = workItem.work;
    newWorkItem.status = err ? 'error' : 'completed'
    newWorkItem.completedWork = completedWork;
    newWorkItem.error = err;
    return newWorkItem;
}
exports.cloneCompleted = cloneCompleted;


exports.WorkItem = WorkItem;

/**
 * @contructor
 * @param {redis.RedisClient} redisClient
 * @param {String} qname
 */
function WorkQueue(qname, redisClient) {
  var client = redisClient;
  var q = qname;
  this.redisObjects = {list:q};
  
  /**
   * Queueing primatives based on redis lists and Sets
   * queues are queues of item, with ids
   */

  /**
   * @param {Object|WorkItem}   item
   * @param {Function} callback
   */
  this.enqueue = function(item, callback){
    if (item instanceof WorkItem){
      workItem = item
    } else {
      var workItem = new WorkItem(item);
    }
    client.rpush(q, JSON.stringify(workItem), function(err, length){
      if (!err) {
        callback(null, {id:workItem.id, queueLength:length});
      } else {
        // redis error
        callback(err);
      }
    });
  }
  
  /**
   * @param {Function} callback with head WorkItem
   */
  this.dequeue = function(callback){
    client.lpop(q, function(err, workItemStr){
      if (!err){
        callback(null, JSON.parse(workItemStr));
      } else {
        callback(err);
      }
    }); 
  }
  
  /**
   * @param {Function} callback with count
   */
  this.queueLength = function(callback){
    client.llen(q, callback);
  }
  
  /**
   * Empties the queue of all items
   */
  this.empty = function(callback){
    client.del(q, callback);
  }
  
  /**
   * Returns an observable to the head of a q
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
      })
      return function(){};
    });
  }  
  /**
   * Co-operative 1-time dequeue
   * @return {Rx.Observable}
   */
  this.drainRx = function(){
    var self = this;
    var rc = client;
    return Rx.Observable.Create(function(obs){
      // snapshot the current queue length
      rc.llen(q, function(err, length){
        // for as max as we have read length
        // reactively dequeu, assuming other workers
        // are also dequeuing
        var count = length;
        if (!err) {
          Rx.Observable.While(
            function(){return count-- > 0;},
            // apply a minimum time spacing
            self.dequeueRx()
          ).Subscribe(function(workItem){
              obs.OnNext(workItem);
          });
        } else {
          obs.OnError(err);
        }
     });
     return function(){};
    });
  }
}

exports.WorkQueue = WorkQueue;

/**
 * @constructor
 * @param {String} setName
 * @param {redis.RedisClient} redisClient
 */
function WorkSet(setName, redisClient) {
  var client = redisClient;
  var s = setName;
  this.redisObjects = {hmset:s};
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


/**
 * Returns an observable that attempts to empty
 * q from the current time t, assuming other consumers
 * @param {redis.RedisClient} client
 * @param {String} q name
 * @return {Rx.Observable} of WorkItems
 */
function drainQueueRx(client, q){
  return Rx.Observable.Create(function(obs){
    // snapshot the current queue length
    client.llength(q, function(err, length){
      // for as max as we have read length
      // reactively dequeu, assuming other workers
      // are also dequeuing
      var count = length;
      if (!err) {
        Rx.Observable.While(
          function(){return count-- >= 0;},
          dequeueRx(client, q)
        )
      } else {
        obs.OnError(err);
      }
   });
   return function(){};
  })
  // filter for blanks as others may have
  // drained ahead
  .Where(function(item){return item != null;});
}
exports.drainQueueRx = drainQueueRx;
