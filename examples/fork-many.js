// ToDo: Add fork may example

var Rx = require('rx').Rx;
var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;   

/**
 * Forking Rx combinator that projects reactive stream onto distributed
 * work queue
 */
Rx.Observable.prototype.ForkMany = function(qname, redisOpts){
  var q = new WorkQueueRx(qname, redisOpts)
  return this.SelectMany(function(r){
    q.enqueue(r);
    return q.completedObservable();
  });
}

Rx.Observable.FromArray([1,2,3])
  .ForkMany('test-q')
    .Subscribe(function(result){
      console.log(result);
    });

var worker = new WorkQueueRx('test-q');
worker.workObservable().Subscribe(function(workItem){
  workItem.callback(null, workItem.work + 1);
})