var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');


var WorkQueueRx  = require('../lib/qrx').WorkQueueRx;   

// create a new queue with well known name
wq = new WorkQueueRx('test-wq');
// clear any pending work (optional)
wq.clear();

wq.enqueue('one');
wq.enqueue('two');

// subscribe for work
wq.workObservable().Subscribe(function(workObj){
  // worker receives two an object with work
  // and a callback
  // {work:work callback:callback}
  console.log('new work', workObj.work);
  // callback to mark the work completed or err'd
  // if the callback is not called the work wll remain in the work
  // queue and will be repeatedly sent to the worker
  workObj.callback(null, workObj.work + ' - completed');
});

// subscribe for completed work
wq.completedObservable().Subscribe(function(completedWork){
  console.log('completed work', completedWork)
})
