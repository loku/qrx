var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');


var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;   

wq = new WorkQueueRx('test-wq');
wq.clear();

wq.enqueue('one');
wq.enqueue('two');

wq.workObservable().Subscribe(function(workObj){
  console.log('new work', workObj.work);
  // callback to mark the work completed or err'd
  workObj.callback(null, workObj.work + ' - completed');
});

wq.completedObservable().Subscribe(function(completedWork){
  console.log('completed work', completedWork)
})
