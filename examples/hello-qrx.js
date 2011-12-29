var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');


var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;   

// create a new queue with well known name
wq = new WorkQueueRx('test-wq');
// clear any pending work (optional)
wq.clear();

wq.enqueue('one');
wq.enqueue('two');

// subscribe for work
wq.workObservable().Subscribe(function(workObj){
  console.log('new work', workObj.work);
  // callback to mark the work completed or err'd
  workObj.callback(null, workObj.work + ' - completed');
});

// subscribe for completed work
wq.completedObservable().Subscribe(function(completedWork){
  console.log('completed work', completedWork)
})
