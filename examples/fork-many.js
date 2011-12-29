// ToDo: Add fork may example

var forkmany = require('../lib/forkmany');
var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;  

// add the forkmany combinator
var Rx = forkmany.extendRx(require('rx').Rx);

Rx.Observable.FromArray([1,2,3])
  // ForkMany usage
  .ForkMany('test-q')
    .Subscribe(function(result){
      console.log(result);
    });

var worker = new WorkQueueRx('test-q');
worker.workObservable().Subscribe(function(workItem){
  workItem.callback(null, workItem.work + 1);
})