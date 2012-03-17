var redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');


var WorkQueue  = require('../lib/workqueue').WorkQueue,
    WorkItem = require('../lib/workitem').WorkItem;


exports.testWorkQueue = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1());
  wq.enqueue(1, function(e,o){
    assert.isNull(e);
    wq.length(function(e,l){
      assert.equal(l,1);
    });
    wq.dequeue(function(e, w){
      assert.isNull(e);
      assert.equal(w.work,'1');
    });
  });
}

exports.testWorkItemEnqueue = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1());
  var testItem = new WorkItem('foo');
  wq.enqueue(testItem, function(e,o){
    assert.isNull(e);
    wq.length(function(e,l){
      assert.equal(l,1);
    });
    wq.dequeue(function(e, w){
      assert.isNull(e);
      assert.equal(w.work,'foo');
      assert.equal(w.id, testItem.id);
    });
  });
}


exports.testWorkItemEmpty = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1());
  var testItem = new WorkItem('foo');
  wq.enqueue(testItem, function(e,o){
    assert.isNull(e);
    wq.length(function(e,l){
      assert.equal(l,1);
    });
    wq.empty(function(e, _){
      assert.isNull(e);
      wq.length(function(e, l){
        assert.equal(l,0);
      });
    });
  });
}

exports.testBlockingDequeue = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1());
  var testItem = new WorkItem('foo');
  wq.enqueue(testItem, function(e,o){
    assert.isNull(e);
    wq.length(function(e,l){
      assert.equal(l,1);
       var rc = redis.createClient();
       wq.blockingDequeue(rc, 1000, function(e,r){
        assert.equal(r.work, "foo");
      });
    });
  });
}


exports.testBlockingDequeueRx = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1());
  var testItem = new WorkItem('foo');

  var dequeued = false;
  var completed = false;
  
  wq.enqueue(testItem, function(e,o){
    assert.isNull(e);
    wq.length(function(e,l){
      assert.equal(l,1);
      wq.blockingDequeueRx(100).Subscribe(function(r){
      assert.equal(r.work, 'foo');
      dequeued = true;
    },
    function(err){},
    function(){
      completed = true;
    });

   });
  });

  beforeExit(function(){
    assert.equal(dequeued, true);
    assert.equal(completed, true);
  })
}

exports.testBlockingDrainRx = function(beforeExit, assert) {
  var drainCount = 0;

  var wq = new WorkQueue(uuid.v1());
  var qSize = 10;

  Rx.Observable.Range(1,qSize).Subscribe(function(r){
    wq.enqueue(r, function(e,r){});
  });

  wq.blockingDrainRx().Subscribe(function(r){drainCount++;});

  beforeExit(function(){
    assert.equal(drainCount, qSize);
  });
}

exports.testTxnBlockingDequeueEnqueue = function(beforeExit, assert) {
  var fromq = new WorkQueue(uuid.v1());
  var toq = new WorkQueue(uuid.v1());

  var client = redis.createClient();

  var length;
  var head;

  fromq.enqueue(1, function(e,r){
   fromq.txnBlockingDequeueEnqueue(client, toq.q, 0, function(e,r){
     toq.length(function(e,r){
       length = r;
     })
     toq.peek(function(e,r){
       head = r.work;
     })
   });
  });

  beforeExit(function(){
    assert.equal(length, 1);
    assert.equal(head, 1);
  });

}

setTimeout(function(){process.exit(0)}, 1000);




