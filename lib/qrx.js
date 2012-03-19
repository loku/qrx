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
 * Creates a 'complete' callback function for completed work
 * @param {WorkQueueRx} queue
 * @param {Object) workItem
 * @return {function} completed work callback
 */
function completeCallbackFn(queue, workItem){
  return function(err, completedWork){
    var completedWorkItem = cloneCompleted(workItem, err, completedWork);
    queue.markWorkCompleted(workItem, completedWorkItem);
    queue.decWorkInFlight();
  }
}

/**
 * @constructor
 * Constructs a distributed observable work queue based on redis
 * @param {String} qname logical queue name, multiple workers can operate from
 *                 same queue
 * @param {Object} [redisOpts] redis connection options
 * @param {Integer} throttle max amount of in-flight work to any worker
 * @param {Interger} rate max work frequency in ms
 */
function WorkQueueRx(qname, redisOpts, throttle, rate) {

  // two clients are required to avoid blocking conditions
  var client = newRedisClient(redisOpts);

  this.throttle = throttle; // undefined means no throttle
  //
  // work rate limit
  this.rate = rate || 0;    // 0 means no rate limit

  // count of work in flight
  this.workInFlight = 0;

  this.getWorkInFlight = function(){
    return this.workInFlight;
  }

  this.incWorkInFlight = function(){
    this.workInFlight++;
  }

  this.decWorkInFlight = function(){
    this.workInFlight--;
  }

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
   */
  this.enqueue = function(work) {
    // push onto the queue
    // work contains by name, where completed work should
    var workItem = new WorkItem(work, this.completedQueueName);
    pending.enqueue(workItem, function(err, workItem){
      if (err){
        throw err;
      }
    });
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
    var completed = new WorkQueue(this.completedQueueName, redisOpts);
    return Rx.Observable.Create(function(obs){
      var completedDrain = completed.blockingDrainRx(0,0);
      var heartBeat = Rx.Observable.Interval(1000);
      var subs = heartBeat.Merge(completedDrain).Subscribe(function(r){
        if (!(r instanceof Object)){
          // beat
          if (self.queueStopped) {
            // signal completed if stopped
            obs.OnCompleted();
          }
        } else {
          obs.OnNext(r);
        }
      },
      function(err){
        obs.OnError(err);
      });
      return function(){subs.Dispose();};
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
  this.workObservable = function(){
    var self = this;
    var rc = newRedisClient(redisOpts);
    var stopped = false;

    rc.incr(this.workerCount);

    return Rx.Observable.Create(function(obs){
      var rateInterval = Rx.Observable.Interval(self.rate).Where(function(){
        return  ((self.throttle == undefined) ||
            (self.getWorkInFlight() < self.throttle)) &&
            !stopped
      });
      // asynchronous loop for more work at a timed rate
      var intervalSubs = rateInterval.Subscribe(function(_){
        self.incWorkInFlight();
        pending.blockingDequeue(rc, 0, function(err, workItem){
          if (workItem.work != STOP_MESSAGE) {
            // add it to the working set
            workingSet.addToSet(workItem, function(err, result){
              if (!err){
                // dispatch to the worker
                // workers get only the information they need
                var workObj = {work: workItem.work,
                               callback: completeCallbackFn(self, workItem)};

                obs.OnNext(workObj);

              } else {
                // fail the worker
                obs.OnError(err);
              }
            });
          } else {
            // signal worker complete
            obs.OnCompleted();
            stopped = true;
          }
        });
      });
      return function(){intervalSubs.Dispose(); rc.quit();};
    });

  }

  this.markWorkCompleted = function(workItem, completedWorkItem, callback) {
    // TODO: Set a redis transaction
    var self = this;
    workingSet.removeFromSet(workItem.id, function(err, _) {
      if (!err){
        // abstraction breach
        // deliever the complted work item
        client.lpush(workItem.completedWorkQueue, JSON.stringify(completedWorkItem), function(err, length){
          if (err) {
            // redis error
            // throw
          }
        });
      } else {
        // redis err
        // throw
      }
    });
  };


  /**
   * Resets the stats of the work queue
   */
  this.clear = function(){
    pending.empty();
  }
}

exports.WorkQueueRx = WorkQueueRx;