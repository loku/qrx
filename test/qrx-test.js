var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');

var WorkQueueRx  = require('../lib/qrx').WorkQueueRx;


exports.testWorkQueueRx = function(beforeExit, assert) {
  console.log('Running test - testWorkQueueRx');

  var qname = 'clean-test' + uuid.v1();

  var wq = new WorkQueueRx(qname);

  var WORK_COUNT = 500;

  for(var i=0; i < WORK_COUNT; i++){
    wq.enqueue(i);
  }

  var workReceived = 0;

  // wait some time for all work to be enqueued
  setTimeout(function(){
    wq.workObservable().Subscribe(function(workObj){
    workReceived++;
    workObj.callback(null, workObj.work + 3);
    });
  },5000);

  // subscribe for completed work
  var completedWorkCount = 0;
  wq.completedObservable().Subscribe(function(completedWork){
    completedWorkCount++;
    assert.equal((completedWork.completedWork - completedWork.work) == 3, true);
  })

  beforeExit(function(){
    console.log('validating test - testWorkQueueRx');
    assert.equal(workReceived, WORK_COUNT);
    assert.equal(completedWorkCount, WORK_COUNT);
  })
}





exports.multiWorkQueueRx = function(beforeExit, assert) {
  var qname = 'multiWorkQueueRx' + uuid.v1();
  var wqMaster = new WorkQueueRx(qname);

  var WORK_COUNT = 500;
  console.log('Running test - multiWorkQueueRx');

  for(var i=0; i < WORK_COUNT; i++){
    wqMaster.enqueue(i);
  }

  // two slaves serving 1 master
  var workReceived = 0;
  var slave1 = new WorkQueueRx(qname);

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

  var slave2 = new WorkQueueRx(qname);
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
    console.log('validating test - multiWorkQueueRx');
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

exports.throttleTest = function(beforeExit, assert) {
  console.log('Runing test - throttleTest');

  var qname = 'throttle-test' + uuid.v1();

  var wqMaster = new WorkQueueRx(qname);

  var WORK_COUNT = 500;
  var workCompleted = 0;
  console.log('Test WorkCount', WORK_COUNT);
  for(var i=0; i < WORK_COUNT; i++){
    wqMaster.enqueue(i);
  }

  // two slaves serving 1 master, with different throttles
  var workInFlight1 = 0;

  var slave1 = new WorkQueueRx(qname, null, 10);

  // count of # of workers who have recvd stop
  var stopCount = 0;
  slave1.workObservable().Subscribe(function(workObj){
    workInFlight1++;

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

  var slave2 = new WorkQueueRx(qname, null, 20);
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
      console.log('validating test - throttleTest');
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

/**
 * Validate full q then subscription post everything is queued
 */
exports.queueAllFirstTest = function(beforeExit, assert){
  console.log('Runing test - queueAllFirstTest');
  var qname = 'queueAllFirstTest' + uuid.v1();

  var wqMaster = new WorkQueueRx(qname);

  var WORK_COUNT = 1000;
  var workCompleted = 0;
  var completedWorkCount = 0;

  for(var i=0; i < WORK_COUNT; i++){
    wqMaster.enqueue(i);
  }

  // two slaves serving 1 master
  var workInFlight1 = 0;

  var slave1 = new WorkQueueRx(qname, null, 10);

  // count of # of workers who have recvd stop
  var stopCount = 0;

  setTimeout(function(){
    slave1.workObservable().Subscribe(function(workObj){
    // do some work asynchronously
    workObj.callback(null, workObj.work + 3);
    workCompleted++;

  },
  function(exn){},
  function(){
    stopCount++;
  });
  }, 4000)

  var allWorkCompleted = false;
  wqMaster.completedObservable().Subscribe(function(completedWork){
    completedWorkCount++;
    assert.equal((completedWork.completedWork - completedWork.work) == 3, true);
    if (completedWorkCount == WORK_COUNT){
      wqMaster.stop();
    }
  },
  function(err){
  },
  function(){
    allWorkCompleted = true;
  });

  beforeExit(function(){
    console.log('validating test - queueAllFirstTest');
    console.log('verify all work completed.');
    assert.equal(workCompleted, WORK_COUNT);
    assert.equal(completedWorkCount, WORK_COUNT);
  });

}

exports.tooManyCallbacks = function(beforeExit, assert){
  console.log('Runing test - tooManyCallbacks');
  var qname = 'tooManyCallbacks' + uuid.v1();
  var wqMaster = new WorkQueueRx(qname);

  var WORK_COUNT = 5;
  for(var i=0; i < WORK_COUNT; i++){
    wqMaster.enqueue(i);
  }

  var workCompleted = 0;
  var slave1 = new WorkQueueRx(qname, null, 2);
  slave1.workObservable().Subscribe(function(workObj){
    workObj.callback(null, workObj.work + 3);
    workObj.callback(null, workObj.work + 3);
    workObj.callback(null, workObj.work + 3);
    workCompleted++;
  })

  beforeExit(function(){
    console.log('validating test - tooManyCallbacks');
    assert.eql(workCompleted, WORK_COUNT, 'all work received');
    assert.eql(slave1.getWorkInFlight(), 0, 'work in flight is 0');
  });
}

setTimeout(function(){process.exit(0)}, 20000);



