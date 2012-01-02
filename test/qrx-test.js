var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');
var WorkQueueRx  = require('../lib/qrx').WorkQueueRx;   


exports.testWorkQueueRx = function(beforeExit, assert) {
  var wq = new WorkQueueRx('clean-test');
  wq.clear();

  var WORK_COUNT = 500;
  console.log('Test WorkCount', WORK_COUNT);
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



exports.multiWorkQueueRx = function(beforeExit, assert) {
  var wqMaster = new WorkQueueRx('clean-test2');
  wqMaster.clear();

  var WORK_COUNT = 500;
  console.log('Test WorkCount', WORK_COUNT);
  for(var i=0; i < WORK_COUNT; i++){
    wqMaster.enqueue(i);
  }
  
  // two slaves serving 1 master
  var workReceived = 0;
  var slave1 = new WorkQueueRx('clean-test2');

  // count of # of workers who have recvd stop
  var stopCount = 0;
  slave1.workObservable().Subscribe(function(workObj){
    workReceived++;
    workObj.callback(null, workObj.work + 3);
  }, 
  function(exn){},
  function(){
    stopCount++;
  });
  
  var slave2 = new WorkQueueRx('clean-test2');
  slave2.workObservable().Subscribe(function(workObj){
    workReceived++;
    workObj.callback(null, workObj.work + 3)
  },
  function(exn){},
  function(){
    stopCount++;
  });
  
  // master get's his work
  var completedWorkCount = 0;
  var errCount = 0;
  var allWorkCompletedSignal = false;
  
  wqMaster.completedObservable().Subscribe(function(completedWork){
    completedWorkCount++;
    assert.equal((completedWork.completedWork - completedWork.work) == 3, true);
    if (completedWorkCount == WORK_COUNT){
      wqMaster.stop();
    }
  },
  function(err){
    console.log('some error');
    errCount++;
  },
  function(){
    console.log('all work completed');
    allWorkCompletedSignal = true;
  });
  
  
  beforeExit(function(){
    console.log('verify all work received.');
    assert.equal(workReceived, WORK_COUNT);
    console.log('verify all work completed.');
    assert.equal(completedWorkCount, WORK_COUNT);
    console.log('verify completed recieved complete.');
    assert.equal(allWorkCompletedSignal, true);
    console.log('verify that all workers have been stopped')
    assert.equal(stopCount, 2);
  })
}



setTimeout(function(){process.exit(0)}, 5000);



