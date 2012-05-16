
var forkmany = require('../lib/rx-extensions');
var Qrx  = require('../lib/qrx');

// add the forkmany combinator to Rx
var Rx = forkmany.extendRx(require('rx').Rx);

Rx.Observable.FromArray([1,2,3])
  // ForkMany usage
  .ForkMany({qname:'test-q'})
    .Subscribe(function(result){
      console.log(result);
    });

var worker = new Qrx({qname:'test-q'});
worker.workObservable().Subscribe(function(workItem){
  workItem.callback(null, workItem.work + 1);
});