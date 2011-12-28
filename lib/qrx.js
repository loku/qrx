var uuid = require('node-uuid'), 
    redis = require('redis'),
    Rx = require('rx').Rx;

var work  = require('./work.js'),
    WorkItem = work.WorkItem,
    WorkQueue = work.WorkQueue,
    WorkSet = work.WorkSet;  
    cloneCompleted = work.cloneCompleted;
  
var channels = require('./channels.js'),
    Channel  = channels.Channel,
    ns       = channels.ns;

/**
 * Creates a callback function for completed work, closing in queue env
 * @param {WorkQueueRx} queue 
 * @param {Object) workItem
 * @return {function} completed work callback
 */
function callbackFn(queue, workItem){
  return function(err, completedWork){
    var completedWorkItem = cloneCompleted(workItem, err, completedWork);    
    queue.markWorkCompleted(workItem, completedWorkItem);
  }
}

/**
 * @constructor
 * Constructs a distributed observable work queue based on redis
 * @param {String} qname logical queue name, multiple workers can operate from
 *                 same queue
 * @param {Object} [redisOpts] redis connection options
 */
function WorkQueueRx(qname, redisOpts) {
  
  this.redis = redisOpts ?
               redis.createClient(redisOpts.port, redisOpts.host, redisOpts):
               redis.createClient();
 
 
  // check to see if there is already a queue, throw error
  // well known shared work channels
  // server-side
  var pending       = new WorkQueue(ns(qname, 'pending'),   this.redis);                 
  var working       = new WorkSet(ns(qname, 'working'),     this.redis);
  
  // channels
  var workChannel          = new Channel(ns(qname, 'work'));
  
  
  
  // unique id for this client instance
  // any work send on the workChannel will be stamped with
  // the client id
  var clientId      = uuid.v1(); 
  // client-side
  var completedChannel     = new Channel(ns(qname, 'completed'));
  // dedicated queu and  
  var completed            = new WorkQueue(ns(qname, 'completed'), this.redis);
  
 
  
  // ToDo: for every client set a channel and make the 
  //       the channels dynamic so that workers can broadcast
  //       to multiple clients
  
  
  // dictionary of redis objects for debugging
  this.redisObjects = {pending:pending.redisObjects,
                       working:working.redisObjects,
                       completed:completed.redisObjects,
                       workChannel:workChannel.redisObjects,
                       completedChannel:completedChannel.redisObjects};
 
 
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
  /**
   * Adds work to queue for workers
   * @param {Object} work
   */
  this.enqueue = function(work) {
    // push onto the queue
    pending.enqueue(work, function(err, workItem){
      if (!err){
        // signal waiting workers of new work in queue
        workChannel.sendMessage({id:workItem.id, status:'pending'});
      }
    });  
  };
  
  /**
   * Returns the observable of completed work items
   * @return {Rx.Observable}
   */
  this.completedObservable = function() {
    var heartBeat = Rx.Observable.Interval(1000);
    // snapshot train of any pending work
    function unpackCompletedWork(completedWorkItem){
      // strip internal representation
      return {work:completedWorkItem.work,
              completedWork: completedWorkItem.completedWork, 
              status:completedWorkItem.status,
              error:completedWorkItem.error} ;
    }
    
    var pendingCompleted = completed.drainRx().Select(function(completedWorkItem){
              return unpackCompletedWork(completedWorkItem);
            });
    
    // signaled pending work
    // project the work messages onto work, onto reactive dequeues   
    var workStream = workChannel.asObservable().Merge(heartBeat)
                      .SelectMany(function(_){
                        return pendingCompleted;
                      });
                      
    
    
    // pending will be hot, so will be handled mostly
    // ahead of new work
    return workStream;
           
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
              callback(null, workItem);
            } else {
              callback(err, null);
            }
          });
        } else {
          // no owrk to do
          callback(null, null)
        }
      } else {
        callback(err, null);
      }
    });
  };
  
  this.markWorkCompleted = function(workItem, completedWorkItem, callback) {
    // TODO: Set a redis transaction
    working.removeFromSet(workItem.id, function(err, _){
      if (!err){
        completed.enqueue(completedWorkItem, function(err, _){
          if (!err) {
            completedChannel.sendMessage({id:completedWorkItem.id, 
                                          status:completedWorkItem.status});
          } else {
            // redis err
          }
        });
      } else {
        // redis err
      }
    });
  };
  
  this.workObservable = function(){
    // ToDo: Add completed signal
    var heartBeat = Rx.Observable.Interval(1000);
    var self = this;
    
    var nextWork = Rx.Observable.Create(function(obs){
      self.nextWorkScheduled(function(err, workItem){
        // will get a callback of null if there is no work todo
        if (!err){
          if (workItem){
            // signal waiting workers of new work in queue
            workChannel.sendMessage({id:workItem.id, status:'working'}, function(e,o){});
            // callers get only the information they need
            var workObj = {work: workItem.work,
                           callback: callbackFn(self, workItem)};
                         
            // wrap the call to worker in exception handler
            try {
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
      return function(){};
    })
    
    
    function pendingWork(){
      return Rx.Observable.Create(function(obs){
        pending.queueLength(function(err, length){
          if (!err){
            var count = length;
            Rx.Observable.While(
              function(){return count-- > 0;},
              nextWork
            ).Subscribe(function(workItem){
               obs.OnNext(workItem); 
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
    
    var workStream = workChannel.asObservable().Merge(heartBeat).SelectMany(function(_){
      return pendingWork().Concat(nextWork);
    });
    
    return workStream;
  }
  
  /**
   * Resets the stats of the work queue
   */
  this.clear = function(){
    pending.empty();
    working.empty();
    completed.empty();
  }
}

exports.WorkQueueRx = WorkQueueRx;
