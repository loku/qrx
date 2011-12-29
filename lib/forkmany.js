var Rx = require('rx').Rx;
var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;   

/**
 * Forking Rx combinator that projects reactive stream onto distributed
 * work queue
 */
function extendRx(Rx){
  Rx.Observable.prototype.ForkMany = function(qname, redisOpts){
    var q = new WorkQueueRx(qname, redisOpts)
    return this.SelectMany(function(r){
      q.enqueue(r);
      return q.completedObservable();
    });
  }
  return Rx;
}

exports.extendRx = extendRx;