var redis = require('redis'),
    expect = require('chai').expect,
    sinon = require('sinon'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid'),
    Q = require('q');


var WorkQueue  = require('../lib/workqueue').WorkQueue,
    WorkItem = require('../lib/workitem').WorkItem;


describe('WorkQueue', function() {
  it ('should enqueue work item', function() {
    var wq = new WorkQueue(uuid.v1());
    var workItem = new WorkItem('foo');
    return Q.node(wq.enqueue, wq, workItem)()
    .then(function(){
      return Q.node(wq.length, wq)();
    })
    .then(function(length){
      expect(length).to.equal(1);
      return Q.node(wq.dequeue, wq)();
    })
    .then(function(headValue) {
      expect(headValue.work).to.equal('foo');
    });
  })
 
  it ('should enqueue work values as work items', function() {
    var wq = new WorkQueue(uuid.v1());
    return Q.node(wq.enqueue, wq, 1)()
    .then(function(){
      return Q.node(wq.length, wq)();
    })
    .then(function(length){
      expect(length).to.equal(1);
      return Q.node(wq.dequeue, wq)();
    })
    .then(function(headValue) {
      expect(headValue.work).to.equal(1);
    });
  });

  it('should blocking dequeue', function() {
    var wq = new WorkQueue(uuid.v1());
    var deferredDeq = Q.defer();
    // setup the blockng dequeue
    var rc = redis.createClient();
    wq.blockingDequeue(rc, 1000, function(e,r) {
        expect(r.work).to.equal('foo');
        deferredDeq.resolve(r);
    });
    return Q.all([Q.node(wq.enqueue, wq, 'foo')(), deferredDeq.promise]);
  });

  it ('should blocking dequeue Rx', function() {
    var wq = new WorkQueue(uuid.v1());
    var deferredDeq = Q.defer();

  });
});


