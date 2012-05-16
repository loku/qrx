var Rx = require('rx').Rx;
var WorkQueueRx  = require('../lib/qrx.js');   


/**
 * Forking Rx combinator that projects reactive stream onto distributed
 * work queue
 */
function extendRx(Rx){
  
  Rx.Observable.prototype.ForkMany = function(options){
    var q = new WorkQueueRx(options)
    return this.SelectMany(function(r){
      q.enqueue(r);
      return q.completedObservable();
    });
  }
  
  /**
   * Rx Combinator that takes an async function with arbitrary paramaeters
   * and yeilds an observable that yields the result of the callback
   * @param {function}asyncFn with variable params
   * @return {Rx.Observable} An observable with the results of the callback
   * 
   */
  Rx.Observable.Callback = function(asyncFn){
    var args = Array.prototype.slice.call(arguments);
    args = args.slice(1);
    return this.Create(function(obs){
      args[args.length] = function(err, result){
        if (!err){
          obs.OnNext(result);
          obs.OnCompleted();
        } else {
          obs.OnError(err);
        }
      };
      asyncFn.apply(null, args)
      return function(){};
    });
  }
  return Rx;
}
exports.extendRx = extendRx;