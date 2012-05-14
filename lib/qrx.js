var uuid = require('node-uuid'),
    redis = require('redis'),
    Rx = require('rx').Rx;

var WorkItem = require('./workitem').WorkItem,
    WorkQueue  = require('./workqueue').WorkQueue,
    WorkSet = require('./workset').WorkSet,
    cloneCompleted = require('./workitem').cloneCompleted,
    newRedisClient = require('./redisutils').newRedisClient,
    ns = require('./messaging').ns;

/**
 * Wraps a node style callback in a timeout timer
 * @param {function} callback
 * @param {Integer} delay
 */
function callbackWithTimeout(callback, delay) {
  // light the fuse
  var timeoutId  = setTimeout(function(){
    timedCallback('timeout', null);
  }, delay);

  var timedCallback =  function(err, result) {
    // cut the red wire
    clearTimeout(timeoutId);
    callback(err, result);
  };
  return timedCallback;
}

/**
 * Creates a 'complete' callback function for completed work
 * @param {WorkQueueRx} queue
 * @param {Object) workItem
 * @return {function} completed work callback
 */
function completeCallbackFn(queue, workItem, scheduleNextWorkFn){
  return function(err, completedWork){
    var completedWorkItem = cloneCompleted(workItem, err, completedWork);
    queue.markWorkCompleted(workItem, completedWorkItem, scheduleNextWorkFn);
  }
}

/**
 * @constructor
 * Constructs a distributed observable work queue based on redis
 * @param {String} qname logical queue name, multiple workers can operate from
 *                 same queue
 * @param {Object} [redisOpts] redis connection options
 * @param {Integer} throttle max amount of in-flight work to any worker
 */
function WorkQueueRx(qname, redisOpts, throttle, completedThrottle, workTimeout) {

  // two clients are required to avoid blocking conditions
  var client = newRedisClient(redisOpts);

  // max time for each work item to complete
  this.workTimeout = workTimeout;

  // count of work in flight
  // default only 1 in flight at any given time per worker
  this.throttle = throttle || 1;

  // ToDo: This assumes 1 subscriber for completed work
  this.completedInFlight = 0;
  this.completedThrottle = completedThrottle || 1;


  // master-side
  var pending       = new WorkQueue(ns(qname, 'pending'), redisOpts);
  var workingSet    = new WorkSet(ns(qname, 'working'), redisOpts);

  // work from the client is routed by the instance id
  var clientId      = uuid.v1();

  // all work enqueued by the client, is return to this client
  this.completedQueueName = ns(qname, 'completed') + clientId;

  this.workerCount = ns(qname, 'worker-count');

  var STOP_MESSAGE  = "QRX_STOP_QUEUE"

  this.queueStopped = false;

  /////////////////
  // Master Side //
  /////////////////

  /**
   * Adds work to queue for workers
   * @param {Object} work
   * @param {function} callback
   */
  this.enqueue = function(work, callback) {
    // push onto the queue
    // work contains by name, where completed work should
    var defaultCallback = function(err, workItem){
      if (err){
        throw err;
      }
    };

    callback = callback || defaultCallback;
    var workItem = new WorkItem(work, this.completedQueueName);
    pending.enqueue(workItem, callback);
  };

  /**
   * Halts all subscribed queue workers and sends completed to subscribers
   * of completed work
   */
  this.stop = function() {
    // stop the client side
    // heart beat will stop due to completed flag
    this.queueStopped = true;
    client.get(this.workerCount, function(err, count){
      for (var i = 0; i < count; i++){
        // put as many stop messages as needed to term workers
        pending.enqueue(STOP_MESSAGE, function(err, workItem){});
      }
    });
  }


  /**
   * Returns the observable of completed work items
   * from the clients completed queue, signaled on the completed channel
   * @return {Rx.Observable}
   */
  this.completedObservable = function() {
    var self = this;
    var rc = newRedisClient(redisOpts);
    var pending = new WorkQueue(this.completedQueueName, redisOpts);
    
    return Rx.Observable.Create(function(obs) {
      function getNextCompleted() {
        while(self.completedInFlight < self.completedThrottle){
          self.completedInFlight++
          pending.blockingDequeue(rc, 0, function(err, completedWorkItem){
            if (!err){
              obs.OnNext(completedWorkItem);
              self.completedInFlight--;
              if (!self.queueStopped){
                Rx.Observable.Start(getNextCompleted);
              } else {
                obs.OnCompleted();
              }
            } else {
             obs.OnError(err);
            }
          });
        }
      }
      Rx.Observable.Start(getNextCompleted);
      return function(){rc.quit();};
    });

  };

  /////////////////
  // Worker Side //
  /////////////////

  /**
   * Returns an observable stream of co-operative work
   * with every Rx subscription creating a new reactive work scream
   * @return {Rx.Observable} stream of work to do
   */
  this.workObservable = function() {
    // immediately 'complete' any workers who subscribe
    // to a stopped queue
    if (this.queueStopped) {
      return Rx.Observable.Empty();
    }
    var self = this;
    var rc = newRedisClient(redisOpts);
    rc.incr(this.workerCount);

    // each work subscription keeps track of how much work
    // is in flight
    var workInFlight = 0;
    return Rx.Observable.Create(function(obs) {
      function getNextWork(){
        // this will set n callbacks where n == throttle
        while (workInFlight < self.throttle){
          workInFlight++;
          pending.blockingDequeue(rc, 0, function(err, workItem){
            if (workItem.work != STOP_MESSAGE) {
              // add it to the working set
              workingSet.addToSet(workItem, function(err, result){
                if (!err){
                  // dispatch to the worker
                  // workers get only the information they need
                  var workObj = {work: workItem.work,
                                 callback:callbackWithTimeout(completeCallbackFn(self,
                                                             workItem,
                                                             function() {
                                                              workInFlight--;
                                                              Rx.Observable.Start(getNextWork);}
                                                             ), self.workTimeout)};
                  // catch any work exception
                  try {
                    obs.OnNext(workObj);
                  } catch (exn){
                    // set the exception and mark the work complete
                    workObj.callback(exn, null);
                    // Rx next tick self schedule
                    Rx.Observable.Start(getNextWork);
                  }

                } else {
                  // fail the worker
                  obs.OnError(err);
                }
              });
            } else {
              // signal worker complete
              obs.OnCompleted();
            }
          });
        }
      }
      // initial kick-off
      Rx.Observable.Start(getNextWork);
      return function(){rc.quit();};
    });

  }

  this.markWorkCompleted = function(workItem, completedWorkItem, scheduleNextWorkFn) {
    // TODO: Set a redis transaction
    var self = this;
    workingSet.removeFromSet(workItem.id, function(err, removedCount) {
      // ingnore subsequent repeated completion calls
      if (!err && (removedCount > 0)){
        // abstraction breach
        // deliever the complted work item
        client.lpush(workItem.completedWorkQueue, JSON.stringify(completedWorkItem), function(err, length){
          if (!err) {
            scheduleNextWorkFn();
          }
        });
      } else {
        // redis err
        // throw
        if (!err){
          //console.warn('warn: Multiple calls to mark completed');
        }
      }
    });
  };

  this.setThrottle = function(t) {
    this.throttle = t;
  }

  /**
   * Resets the stats of the work queue
   */
  this.clear = function(){
    pending.empty();
  }
}

exports.WorkQueueRx = WorkQueueRx;
