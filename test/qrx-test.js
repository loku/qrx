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

  setTimeout(function(){

    wq.workObservable().Subscribe(function(workObj){
    workReceived++;
    workObj.callback(null, workObj.work + 3);
    });
  },10000);

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

exports.thottleTest = function(beforeExit, assert) {
  var wqMaster = new WorkQueueRx('thottle-test');
  wqMaster.clear();

  var WORK_COUNT = 500;
  var workCompleted = 0;
  console.log('Test WorkCount', WORK_COUNT);
  for(var i=0; i < WORK_COUNT; i++){
    wqMaster.enqueue(i);
  }

  // two slaves serving 1 master
  var workInFlight1 = 0;

  var slave1 = new WorkQueueRx('thottle-test', null, 10);

  // count of # of workers who have recvd stop
  var stopCount = 0;
  slave1.workObservable().Subscribe(function(workObj){
    workInFlight1++;
    console.log('work in flight 1:', workInFlight1, 'qrx work in flight:', slave1.workInFlight, 'qrx throttle', slave1.throttle);
    assert.equal(workInFlight1 <= 10, true, 'in flight under throttle');
    // do some work asynchronously
    setTimeout(function(){
      workObj.callback(null, workObj.work + 3);
      workCompleted++;
      workInFlight1--;
    },1000);
  },
  function(exn){},
  function(){
    stopCount++;
  });

  var workInFlight2 = 0;

  var slave2 = new WorkQueueRx('throttle-test', null, 20);
  slave2.workObservable().Subscribe(function(workObj){
    workInFlight2++;
    assert.equal(workInFlight2 <= 20, true);
    // do some work asynchronously
    setTimeout(function(){
      workObj.callback(null, workObj.work + 3);
      workCompleted++;
      workInFlight2--;
    },100);



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
    setTimeout(function(){
      console.log('verify all work received.');
      assert.equal(workCompleted, WORK_COUNT);
      console.log('verify all work completed.');
      assert.equal(completedWorkCount, WORK_COUNT);
      console.log('verify completed recieved complete.');
      assert.equal(allWorkCompletedSignal, true);
      console.log('verify that all workers have been stopped')
      assert.equal(stopCount, 2);
    },2000);
  })
}




setTimeout(function(){process.exit(0)}, 15000);



