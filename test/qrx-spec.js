var expect = require('chai').expect,
    sinon = require('sinon'),
    Q = require('q'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');


var WorkQueueRx  = require('../lib/qrx').WorkQueueRx;

// creates promise to enqueue n count random work on a given q
function testEnqueuePromise(wq, n) {
  var promises = [];
  var randomWork = Math.round(Math.random() * 100000);
  for (var i=0; i < n; i++) {
    promises.push(Q.ncall(wq.enqueue, wq, randomWork));
  }
  return Q.all(promises);
}

function testWorkPromise(wq, n) {
  // do some work
  var workReceived = 0;
  var workPromise = Q.defer();
  wq.workObservable().Subscribe(function(workObj) {
    workReceived++;
    workObj.callback(null, workObj.work + 3);
    if (workReceived == n) {

      workPromise.resolve(workReceived)
    } 
  });
  return workPromise.promise;
}

/**
 * Creates promise to receive n work and complete
 */
function testCompletedPromise(wq, n, expectation) {
  var completedWorkPromise = Q.defer();
  var completedWorkCompleted = Q.defer();
  var completedWorkCount = 0;
  wq.completedObservable().Subscribe(function(completedWork) {
    //console.log('completed work promise');
    completedWorkCount++;
    if (completedWorkCount == n) {
      // apply the expecation fn if provided
      if (expectation){
        expectation(completedWork)
      }
      completedWorkPromise.resolve(completedWorkCount);
      wq.stop();
    }
  },
  function(err){
    throw err;
  },
  function(){
    // promise the validates that completeWork obs has
    // completed
    completedWorkCompleted.resolve(true);
  });
  return Q.all([completedWorkPromise.promise, completedWorkCompleted.promise]);
}


describe('QRX', function(){
  it('should q all items, work on them, have correct results and complete',
    function() {
    var workCount = 10;
    var qname = 'clean-test' + uuid.v1();
    var wq = new WorkQueueRx(qname);

    // subscribe for completed work and check
    // results
    // run everything concurrently
    return Q.all([testEnqueuePromise(wq, workCount),
                  testWorkPromise(wq, workCount),
                  testCompletedPromise(wq, workCount, function(completedWork){
                    expect(completedWork.completedWork - completedWork.work).to.equal(3);
                  })]);
  });

  it('should allow multiple workes for the same queue', function() {
    var workCount = 1000;
    var qname = 'clean-test' + uuid.v1();
    var wq = new WorkQueueRx(qname, null);

    // run two workers concurrently
    return Q.all([testEnqueuePromise(wq, workCount),
                  testWorkPromise(wq, 50),
                  testWorkPromise(wq, 50),
                  testCompletedPromise(wq, workCount, function(completedWork){
                    expect(completedWork.completedWork - completedWork.work).to.equal(3);
                  })]);

  });

  it('should honor the work and completed throttles', function() {
    var workCount = 10;
    var qname = 'clean-test' + uuid.v1();
    var wq = new WorkQueueRx(qname, null, 5, 5);
    
    var workInFlight = 0;
    var workDefer = Q.defer();
    var workCompleted = 0;
    wq.workObservable().Subscribe(function(workObj) {
      workInFlight++;
      // work in flight should never exceed the throttle
      if(workInFlight > 10) {
        throw 'tooManyInFlight: ' + workInFlight;
      }
      // make work take some time
      setTimeout(function() {
        workObj.callback(null, workObj.work);
        workCompleted++;
        if (workCompleted == workCount) {
          workDefer.resolve(workCompleted)
        }
        workInFlight--;
      }, 100);
    });

    var completedDefer = Q.defer();
    var completedCount = 0;
    var completedInFlight = 0;
    wq.completedObservable().Subscribe(function(completed) {
      completedInFlight++;
      completedCount++;
      
      if (completedInFlight > 5) {
        throw 'tooManyInFlight: ' + completedInFlight;
      }
      if (completedCount == workCount){
        completedDefer.resolve(completedCount);
      }
       completedInFlight--;
    });
    return Q.all([testEnqueuePromise(wq, workCount), completedDefer.promise, workDefer.promise]);
  });

  it ('should be able work on work sitting waiting in the queue before any subscription.', function() {
    var workCount = 20;
    var qname = 'clean-test' + uuid.v1();
    var wq = new WorkQueueRx(qname);
    return testEnqueuePromise(wq, workCount)
            .then(function(){
              return Q.all([testWorkPromise(wq, workCount),
                            testCompletedPromise(wq, workCount)])});
  });

  it ('should ignore multiple complete calls only taking the first result for work', function(){
    var workCount = 3;
    var qname = 'clean-test' + uuid.v1();
    var wq = new WorkQueueRx(qname);
    var workReceived = 0;
    var deferredWork = Q.defer();

    wq.workObservable().Subscribe(function(workObj) {
      workReceived++;
      workObj.callback(null, workObj.work + 3);
      workObj.callback('ug!');
      workObj.callback('ug!');
      if (workReceived == workCount) {
        deferredWork.resolve(workReceived);
      }
    });
    return Q.all([testEnqueuePromise(wq, workCount), 
                  deferredWork.promise,
                  testCompletedPromise(wq, workCount)]);
  });


  it ('should error the timeout on work that exceeds the timeout value', function(){
    var workCount = 10;
    var qname = 'clean-test' + uuid.v1();
    var wq = new WorkQueueRx(qname, null, 1, 1, 100);

    var workReceived = 0;
    var deferredWork = Q.defer();

    // subscribe for work
    wq.workObservable().Subscribe(function(workObj) {
      workReceived++
      // every other work item should take longer than
      // timeout set
      setTimeout(function() {
        workObj.callback(null, workObj.work + 3);
        if (workReceived == workCount) {
          deferredWork.resolve(workReceived);
        }
      }, workReceived % 2 ? 200 : 0);
    })

    // every other
    var completedWorkReceived = 0;
    var deferredCompleted = Q.defer();
    wq.completedObservable().Subscribe(function(completedWork) {
      completedWorkReceived++;
      if (completedWorkReceived % 2) {
        // every even work item should be timedout
        expect(completedWork.error).to.equal('timeout');
      }
      if (completedWorkReceived == workCount){
        deferredCompleted.resolve(completedWorkReceived);
      }
    })
    return Q.all([testEnqueuePromise(wq, workCount),
                  deferredWork.promise,
                  deferredCompleted.promise]);
  });
});
