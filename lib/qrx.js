var uuid = require('node-uuid'),
    redis = require('redis'),
    Rx = require('rx').Rx
    _ = require('underscore'),
    os = require('os');

var WorkItem = require('./workitem'),
    WorkQueue  = require('./workqueue').WorkQueue,
    WorkSet = require('./workset').WorkSet,
    newRedisClient = require('./redisutils').newRedisClient,
    ns = require('./messaging').ns,
    Channel = require('./channels');

/**
 * Wraps a node style callback in a timeout timer
 * @param {function} callback
 * @param {Integer} delay
 */
function callbackWithTimeout(callback, delay) {
  // light the fuse
  var timeoutId  = setTimeout(function() {
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
 * @param {WorkItem) workItem
 * @param {function} scheduleNextWorkFn
 * @return {function} completed work callback
 */
function completeCallbackFn(queue, workItemJSON, scheduleNextWorkFn){
  return function(error, completedWork) {    
    if (workItemJSON.status == 'pending') {
      WorkItem.markCompleted(workItemJSON, error, completedWork);
      queue.markWorkCompleted(workItemJSON, scheduleNextWorkFn);
    }
  }
}

var Qrx = module.exports = function() {
  this.initialize.apply(this, arguments);
}


Qrx.prototype.initialize = function (options) {
  options = options || {};
  
  _.defaults(options, {qname: uuid.v1(),    // default unique q name
                       redisOptions: null,  // local host default
                       workTimeout: Number.MAX_VALUE,
                       workThrottle: 1,
                       completedThrottle: 1});

  
  _.extend(this, options);

  this.completedInFlight = 0;
  
  // two clients are required to avoid blocking conditions
  this.client = newRedisClient(options.redisOptions);

  // every q get's a unique id
  this.clientId = uuid.v1();

  this.pending = new WorkQueue(ns(this.qname, 'pending'), this.redisOptions);

  this.workingSet = new WorkSet(ns(this.qname, 'working'), this.redisOptions);

  // all work enqueued by the client, is return to this client
  this.completedQueueName = ns(this.qname, 'completed') + this.clientId;

  // global redis count of workers on this queue
  this.workerCount = ns(this.qname, 'worker-count');

  this.STOP_MESSAGE  = "QRX_STOP_QUEUE"

  this.queueStopped = false;

  this.statsChannel = new Channel({name:this.qname, redisOptions:options.redisOptions});

  // setup stat counters
  this.stats = {
    pendingWorkCount:0,
    completedWorkCount:0,
    avgWorkTime:0,
    completedWorkBytes:0,
    errorCount:0,
    lastError:null,
    workerId:this.clientId,
    workerHost:os.hostname()
  };
  
  // start the stats heartbeat
  var self = this;
  this.statsHeartbeat = Rx.Observable.Interval(1000).Subscribe(function(tick) {
    self.statsChannel.send(self.stats);
  });

}

/**
* Adds work to queue for workers
* @param {Object} work
* @param {function} callback
*/
Qrx.prototype.enqueue = function(work, callback) {
  // push onto the queue
  // work contains by name, where completed work should
  var defaultCallback = function(err, workItem){
    if (err){
      throw err;
    }
  };
  callback = callback || defaultCallback;
  this.stats.pendingWorkCount++;
  this.pending.enqueue(new WorkItem({work: work, 
                                     completedWorkQueue: this.completedQueueName}),
                       callback);
};

/**
  * Halts all subscribed queue workers and sends completed to subscribers
  * of completed work
  */
Qrx.prototype.stop = function() {
  var self = this;
  // stop the client side
  // heart beat will stop due to completed flag
  this.queueStopped = true;
  this.client.get(this.workerCount, function(err, count){
    for (var i = 0; i < count; i++){
      // put as many stop messages as needed to term workers
      self.pending.enqueue({work:this.STOP_MESSAGE}, function(err, workItem){});
    }
  });
}

/*
 * Returns the observable of completed work items
 * from the clients completed queue, signaled on the completed channel
 * @return {Rx.Observable}
 */
Qrx.prototype.completedObservable = function() {
  var self = this;
  var rc = newRedisClient(this.redisOptions);
  var pending = new WorkQueue(this.completedQueueName, this.redisOptions);


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
Qrx.prototype.workObservable = function() {
  // immediately 'complete' any workers who subscribe
  // to a stopped queue
  if (this.queueStopped) {
    return Rx.Observable.Empty();
  }
  var self = this;
  var rc = newRedisClient(this.redisOptions);
  rc.incr(this.workerCount);

  // each work subscription keeps track of how much work
  // is in flight
  var workInFlight = 0;
  return Rx.Observable.Create(function(obs) {
    function getNextWork(){
      // this will set n callbacks where n == throttle
      while (workInFlight < self.workThrottle){
        workInFlight++;
        self.pending.blockingDequeue(rc, 0, function(err, workItem){
          if (workItem.work != this.STOP_MESSAGE) {
            // add it to the working set
            self.workingSet.addToSet(workItem, function(err, result){
              if (!err){
                // dispatch to the worker
                // workers get only the information they need
                workItem.startTime = Date.now();
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

Qrx.prototype.updateCounters = function(completedWork, byteCount) {

  // update worker stats
  this.stats.pendingWorkCount--;
  this.stats.completedWorkCount++
  this.stats.avgWorkTime =
    (completedWork.endTime - completedWork.startTime) / this.stats.completedWorkCount;

  if (completedWork.error) {
    this.stats.errorCount++;
    this.stats.lastError = completedWork.error;
  } else {
    this.stats.completedWorkBytes += byteCount
  }

}



Qrx.prototype.markWorkCompleted = function(completedWorkItem,
                                           scheduleNextWorkFn) {
  // TODO: Set a redis transaction
  var self = this;
  this.workingSet.removeFromSet(completedWorkItem.id, function(err, removedCount) {
    // ingnore subsequent repeated completion calls
    if (!err && (removedCount > 0)){
      // abstraction breach
      // deliever the complted work item
      var completedWorkJSON = JSON.stringify(completedWorkItem);
      self.updateCounters(completedWorkItem, completedWorkJSON.length);
      self.client.lpush(completedWorkItem.completedWorkQueue,
                        completedWorkJSON,
                        function(err, length) {
                            if (!err) {
                              scheduleNextWorkFn();
                            }
                          });
   } else {
    // redis err throw
    if (!err){
      //console.warn('warn: Multiple calls to mark completed');
    }
   }
 });
};

Qrx.prototype.setWorkThrottle = function(t) {
  this.workThrottle = t;
}

/**
 * Resets the stats of the work queue
 */
Qrx.prototype.clear = function(){
  this.pending.empty();
}

Qrx.prototype.statsObservable = function(period) {
  // returns the rolled-up stats for the Qrx cluster
  period = period || 3000;
  // dictionary of cluster stats updates by workingId
  var qrxStats = {};
  var updateCount = 0;

  // subscribe to the stats channel for every worker
  var statsSubs = this.statsChannel.subscribe(function(statsUpdate){
    function updateSummary(running, nextUpdate) {
      updateCount++;
      running.totalPendingWorkCount += nextUpdate.pendingWorkCount;
      running.totalCompletedWorkCount += nextUpdate.completedWorkCount;
      running.totalErrorCount += nextUpdate.errorCount;
      running.totalErrorRate = running.totalErrorCount/running.totalCompletedWorkCount;
      running.totalCompletedWorkBytes += nextUpdate.completedWorkBytes;
      running.totalAvgWorkTime =  running.totalAvgWorkTime + nextUpdate.avgWorkTime/updateCount;
    }
    
    qrxStats[statsUpdate.workerId] = statsUpdate;
    qrxStats['summary'] = {totalPendingWorkCount: 0,
                           totalCompletedWorkCount: 0,
                           totalErrorCount: 0,
                           totalErrorRate: 0,
                           totalAvgWorkTime: 0,
                           totalCompletedWorkBytes: 0} || qrxStats['summary'];

    updateSummary(qrxStats['summary'], statsUpdate);
  });


  // summarize all values for cluster
  return Rx.Observable.Create(function(obs){
    var statsHeartbeat = Rx.Observable.Interval(period).Subscribe(function(beat) {
     obs.OnNext(qrxStats);
     return function() {statsSubs.Dispose(); statsHeartbeat.Dispose();}
    });
  });
}

