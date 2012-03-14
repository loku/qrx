var uuid = require('node-uuid'), 
    redis = require('redis'),
    Rx = require('rx').Rx;

var work  = require('./work'),
    WorkItem = work.WorkItem,
    WorkQueue = work.WorkQueue,
    WorkSet = work.WorkSet;  
    cloneCompleted = work.cloneCompleted;
    enqueue = work.enqueue;
    dequeue = work.dequeue;
    drainRx = work.drainRx;
    dequeueRx = work.dequeueRx;
  
var channels = require('./channels'),
    Channel  = channels.Channel;
    sendMessage = channels.sendMessage;
    createObservableChannel = channels.createObservableChannel;
    
var messaging = require('./messaging');
    qrxChannelName = messaging.qrxChannelName,
    ns = messaging.ns;
    

/**
 * Stopable Heartbeat class
 * @constructor
 * @param {Integer} interval
 */
function HeartBeat(interval){
  this.interval = interval;
  this.beating = true;
  this.stop = function(){
    this.beating = false;
  }
  /**
   * Returns hearbeat
   * @return {Rx.Observable} of beats
   */
  this.asObservable = function(){
    var self = this;
    return Rx.Observable.GenerateWithTime(
       0,
       function(_)    {return self.beating;}, 
       function(beat) {return beat + 1;},    
       function(beat) {return beat;},      
       function(_)    {return interval;}
    );
  }
}



/**
 * Creates a callback function for completed work, closing in queue env
 * @param {WorkQueueRx} queue 
 * @param {Object) workItem
 * @return {function} completed work callback
 */
function callbackFn(queue, workItem){
  return function(err, completedWork) {
    var completedWorkItem = cloneCompleted(workItem, err, completedWork);    
    queue.markWorkCompleted(workItem, completedWorkItem);
    queue.workinflight--;
  }
}

/**
 * @constructor
 * Constructs a distributed observable work queue based on redis
 * @param {String} qname logical queue name, multiple workers can operate from
 *                 same queue
 * @param {Object} [redisOpts] redis connection options
 */
function WorkQueueRx(name, throttle, redisOpts) {
  
  this.redis = redisOpts ?
               redis.createClient(redisOpts.port, redisOpts.host, redisOpts):
               redis.createClient();
 
  this.workinflight = 0;

  // check to see if there is already a queue, throw error
  // well known shared work channels
  // server-side
  var pending       = new WorkQueue(ns(qname, 'pending'),   this.redis);                 
  var working       = new WorkSet(ns(qname, 'working'),     this.redis);
  
  // channels
  var workChannel   = new Channel(qrxChannelName(qname, 'work'));
  
  QRX_STOP_MSG = 'QRX_STOP';
  
  
  // unique id for this client instance
  // any work send on the workChannel will be stamped with
  // the client id
  var clientId      = uuid.v1();
  this.completedChannelName = qrxChannelName(qname, 'completed') + clientId; 
  this.completedQueueName = ns(qname, 'completed') + clientId;
  
 
  // ToDo: for every client set a channel and make the 
  //       the channels dynamic so that workers can broadcast
  //       to multiple clients
  
  
  // dictionary of redis objects for debugging
  this.redisObjects = {pending:pending.redisObjects,
                       working:working.redisObjects,
                       workChannel:workChannel.redisObjects};
 
 
  /**
   * Rollback is called by the admin on 1 and only 1
   * worker, which will insure the queue state is consistent
   * 
   * ToDo: check for live workers and throw exception, if
   *       rolling back with other live workers
   */
  this.rollback = function(){
    // rollback any all work in the workingSet to pending   
  };
  
  
  /////////////////
  // Client Side //
  /////////////////
  this.completedWorkStopped = false;
  
  /**
   * Adds work to queue for workers
   * @param {Object} work
   */
  this.enqueue = function(work) {
    // push onto the queue
    // work contains by name, where completed work should
    var workItem = new WorkItem(work, this.completedQueueName, this.completedChannelName);
    pending.enqueue(workItem, function(err, workItem){
      if (!err){
        // signal waiting workers of new work in queue
        workChannel.sendMessage(workItem.id + '-pending');
      }
    });  
  };
  
  
  /**
   * Halts all subscribed queue workers and sends completed to subscribers
   * of completed work
   */
  this.stop = function() {
    // ToDo: Client won't stop if they miss mesage, need persistent channel
    //workChannel.sendMessage(QRX_STOP_MSG);
    
    // stop the client side
    // heart beat will stop due to completed flag
    this.completedWorkStopped = true;
    // channel wil oncompleted due to close message 
    sendMessage(this.redis, this.completedChannelName, channels.CLOSE_MSG);

    // all workers will get the close message
    workChannel.close();
    
    
  }
  
  
  /**
   * Returns the observable of completed work items
   * from the clients completed queue, signaled on the completed channel
   * @return {Rx.Observable}
   */
  this.completedObservable = function() {
    var self = this;    
    // snapshot train of any pending work
    function unpackCompletedWork(completedWorkItem){
      // strip internal representation
      return {work:completedWorkItem.work,
              completedWork: completedWorkItem.completedWork, 
              status:completedWorkItem.status,
              error:completedWorkItem.error} ;
    }
    
    var pendingCompleted = drainRx(this.redis, this.completedQueueName)
      .Select(function(completedWorkItem){
        return unpackCompletedWork(completedWorkItem);

   });
    
    // signaled pending work
    // project the work messages onto work, onto reactive dequeues   
    var workStream = createObservableChannel(self.completedChannelName)
                        .SelectMany(function(msg){
                            return dequeueRx(self.redis, self.completedQueueName)
                              .Select(function(workItem){
                                return unpackCompletedWork(workItem);
                              });
                        });
  
    // pending will be hot, so will be handled mostly
    // ahead of new work
    
    // return0
    //   those missed before the queue was listening,
    //   completed work while listening
    //   work missed while I was listening
    return pendingCompleted
            .Concat(workStream)
            .Concat(pendingCompleted);
           
  };
  
  /////////////////
  // Server Side //
  /////////////////  
  this.nextWorkScheduled = function(callback) {
    // TODO: setup a redis transaction
    // dequeue from pending
    pending.dequeue(function(err, workItem){
      if (!err){
        if(workItem){
          working.addToSet(workItem, function(err, _) {
            if (!err) {
              // have workItem
              callback(null, workItem);
            } else {
              // redis err
              callback(err, null);
            }
          });
        } else {
          // no work to do
          // dequeuing can return null
          callback(null, null)
        }
      } else {
        // redis err
        callback(err, null);
      }
    });
  };
  
  this.markWorkCompleted = function(workItem, completedWorkItem, callback) {
    // TODO: Set a redis transaction
    var self = this;
    working.removeFromSet(workItem.id, function(err, _){
      if (!err){
        enqueue(self.redis, workItem.completedWorkQueue, completedWorkItem, function(err, _){
          if (!err) {
            sendMessage(self.redis, workItem.completedWorkChannel, workItem.id + '-item_completed');
          } else {
            // redis err
          }
        });
      } else {
        // redis err
      }
    });
  };
  
  /**
   * Returns an observable stream of co-operative work
   * @return {Rx.Observable} stream of work to do
   */  
  this.workObservable = function(){
    // heartbeat required to clear work from
    // queue in case pub/sub messages don't arrive'
    var heartBeat = new HeartBeat(2000);
    
    var self = this;
    
    
    var nextWork = Rx.Observable.Create(function(obs){
      if (!self.throttle || self.workinflight < self.throttle) {
        self.nextWorkScheduled(function(err, workItem){

          // will get a callback of null if there is no work todo
          if (!err){
            if (workItem){
              // signal waiting workers of new work in queue
              workChannel.sendMessage(workItem.id + '-working', function(e,o){});
              // callers get only the information they need
              var workObj = {work: workItem.work,
                callback: callbackFn(self, workItem)};

        // wrap the call to worker in exception handler
        try {
          self.workinflight++;
          obs.OnNext(workObj);
        } catch(e) {
          // set the error state of on the work
          workObj.callback('worker threw exception' + e, null);
        }
            }
            // complete with no OnNext is the Rx.Observable.Empty
            obs.OnCompleted();
          } else {
            obs.OnError(err);
          }
        });
      }
      return function(){};
    })
    
    // ToDo: This is repeat of a drainQueueX
    function pendingWork(){
      return Rx.Observable.Create(function(obs){
        pending.queueLength(function(err, length){
          if (!err){
            var count = length;
            Rx.Observable.While(
              function(){return count-- > 0;},
              nextWork
            ).Subscribe(function(workItem){
               if (workItem) {
                obs.OnNext(workItem);
               } else {
                // short circuit count, got blank
                count = 0;
                obs.OnCompleted();
               }
            },
            function(exn){
              obs.OnError(exn);
            },
            function(){obs.OnCompleted()});
          } else {
            // dequeue of work failed
            obs.OnError('Could not dequeue work:' + err)
          }
        })
      return function(){};
      });
    }
    
    // subscribe for the closing of the workChannel and stop the
    // work heartbeat on workChannel closing
    workChannel.asObservable().Subscribe(function(r){}, 
                                         function(e){},
                                         function(){heartBeat.stop()});
    
    var workStream = workChannel.asObservable().Merge(heartBeat.asObservable())
        .SelectMany(function(_){
          return nextWork;
        });
    
    // clear any pending work
    return pendingWork().Concat(workStream);
  }
  
  /**
   * Resets the stats of the work queue
   */
  this.clear = function(){
    pending.empty();
    working.empty();
  }
}

exports.WorkQueueRx = WorkQueueRx;
