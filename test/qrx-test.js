var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');
var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;   

exports.testWorkQueueRx = function(beforeExit, assert) {
  var wq = new WorkQueueRx('clean-test');
  wq.clear();

  var WORK_COUNT = 500
  for(var i=0; i < WORK_COUNT; i++){
    wq.enqueue(i);
  }
  
  var workReceived = 0;
  wq.workObservable().Subscribe(function(workObj){
    workReceived++;
    workObj.callback(null, workObj.work + 3);
  });

  var completedWorkCount = 0;
  wq.completedObservable().Subscribe(function(completedWork){
    completedWorkCount++;
    assert.equal((completedWork.completedWork - completedWork.work) == 3, true);
  })
  
  beforeExit(function(){
    assert.equal(workReceived, WORK_COUNT);
    assert.equal(completedWorkCount, WORK_COUNT);
  })
}


exports.testWorkQueueRx = function(beforeExit, assert) {
  var wq = new WorkQueueRx('clean-test');
  wq.clear();

  var WORK_COUNT = 500
  for(var i=0; i < WORK_COUNT; i++){
    wq.enqueue(i);
  }
  
  var workReceived = 0;
  wq.workObservable().Subscribe(function(workObj){
    workReceived++;
    workObj.callback(null, workObj.work + 3);
  });

  var completedWorkCount = 0;
  wq.completedObservable().Subscribe(function(completedWork){
    completedWorkCount++;
    assert.equal((completedWork.completedWork - completedWork.work) == 3, true);
  })
  
  beforeExit(function(){
    assert.equal(workReceived, WORK_COUNT);
    assert.equal(completedWorkCount, WORK_COUNT);
  })
}


setTimeout(function(){process.exit(0)}, 2000);



