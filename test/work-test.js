var vows = require('vows'),
    assert = require('assert'),
    redis = require('redis'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');


var work  = require('../lib/work.js'),
    WorkItem = work.WorkItem,
    WorkQueue = work.WorkQueue,
    WorkSet = work.WorkSet;    



rc = redis.createClient();

exports.testQueueDrainRx = function(beforeExit, assert) {
  var drainCount = 0;
  
  var wq = new WorkQueue(uuid.v1(), rc);
  wq.enqueue('1', function(e,o){});
  wq.enqueue('2', function(e,o){});
  wq.enqueue('3', function(e,o){});
  wq.enqueue('4', function(e,o){});

  wq.drainRx().Subscribe(function(r){drainCount++;});
  beforeExit(function(){
    assert.equal(drainCount, 4);
  });
}

exports.testWorkQueue = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1(), rc);
  wq.enqueue(1, function(e,o){
    assert.isNull(e);
    wq.queueLength(function(e,l){
      assert.equal(l,1);
    });
    wq.dequeue(function(e, w){
      assert.isNull(e);
      assert.equal(w.work,'1');
    });    
  });
}

exports.testWorkItemEnqueue = function(beforeExit, assert){
  var wq = new WorkQueue(uuid.v1(), rc);
  var testItem = new WorkItem('foo');
  wq.enqueue(testItem, function(e,o){
    assert.isNull(e);
    wq.queueLength(function(e,l){
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
  var wq = new WorkQueue(uuid.v1(), rc);
  var testItem = new WorkItem('foo');
  wq.enqueue(testItem, function(e,o){
    assert.isNull(e);
    wq.queueLength(function(e,l){
      assert.equal(l,1);
    });
    wq.empty(function(e, _){
      assert.isNull(e);
      wq.queueLength(function(e, l){
        assert.equal(l,0);
      });
    });    
  });
}


exports.testWorkSet = function(beforeExit, assert){
  var wi = new WorkItem('foo');
  var testSet = new WorkSet(uuid.v1(), rc);
  testSet.addToSet(wi, function(e,o){
    assert.isNull(e);
    testSet.setCount(function(e, count){
      assert.isNull(e);
      assert.equal(count,1);
      testSet.removeFromSet(wi.id, function(e,o){
        assert.isNull(e);
        testSet.setCount(function(e, count){
          assert.equal(count, 0);
        })
      })
    })
  })
};

setTimeout(function(){process.exit(0)}, 1000);







//
///**
// * vow topics are unary and Observable seqences can only
// * return a single value to their test, so the AsynSubject insures
// * the Topic arity
// * @param {Rx.Observable} observable
// * @param {function} callback
// */
//function reactiveTopic(observable, callback){
//  var subject = new Rx.AsyncSubject();
//  var s = observable.Subscribe(function(r){
//    console.log('in subs');
//    subject.OnNext(r);
//    subject.OnCompleted();
//  },
//  function(exn){
//    subject.OnError(exn);
//  });
//  subject.Subscribe(function(topic){
//    console.log('calling back');
//    callback(null, topic);
//    s.Dispose();
//  },
//  function(err){
//    callback(err, null);
//    s.Dispose();
//  });
//}
//
//vows.describe('work queue primatives testing').addBatch({
//  'With a redis connection and new q':{
//    topic:new WorkQueue(redis.createClient(), uuid.v1()),
//    'when you enqueue a WorkItem' : {
//      topic:function(wq){
//        return wq.enqueue('test', this.callback);
//      },
//      'will have length of 1': function(qresult){
//        assert.equal(qresult.queueLength, 1);
//        assert.notEqual(qresult.id, null);
//      },
//      'with a q with one WorkItem, when dequeued': {
//        topic:function(qresult, wq) {
//          return wq.dequeue(this.callback);
//        },
//        'will return the test WorkItem': function(workItem){
//          assert.equal(workItem.work, 'test');
//        },
//        'when q length is called': { 
//          topic: function(workItem, qresult, wq){
//            return wq.queueLength(this.callback);
//          },
//          "the q length will be 0": function(length) {
//            assert.equal(length, 0);
//          } 
//        }
//      }
//    }}
//}).run();
//    
//
//vows.describe('work set primatives testing').addBatch({
//  'with a redis connection and a new set':{
//    topic:{workSet:new WorkSet(redis.createClient(), uuid.v1()), testWork:new WorkItem('test')},
//    'when you add an item to the set':{
//      topic: function(env){
//        env.workSet.addToSet(env.testWork, this.callback);
//      },
//      'and you check the count':{
//        topic: function(_, env) {
//          env.workSet.setCount(this.callback);
//        },
//        'the count will be one': function(count){
//          assert.equal(count,1);
//        },
//        'and when you remove the item':{
//          topic:function(_, __, env) {
//            env.workSet.removeFromSet(env.testWork.id, this.callback)
//          },
//          'the removed item will be returned':function(removedItem){
//           assert.equal(removedItem.work, 'test'); 
//          },
//          'and you check the count': {
//            topic:function(_, __, ___, env) {
//              env.workSet.setCount(this.callback);
//            },
//            'this count will be 0':function(setCount){
//              assert.equal(setCount,0);
//            }
//          }
//        }
//      }
//    }
//  }
//}).run();
//
//
//
////var createObservableChannel = require('../lib/qrx').createObservableChannel;
////var Queue = require('../lib/qrx').Queue;
////vows.describe('qrx Observable Channel testing').addBatch({
////  'With an observable channel':{
////    topic:createObservableChannel('TEST'),
////    "when a message is sent" : {
////      topic:function (ch){
////        reactiveTopic(ch, this.callback);
////        redis.publish('TEST', 'a message');
////      },
////      "it will be received, on the subscription": function(msg) {
////        assert.equal(msg, 'a message')
////      }
////    }
////  }
////});
////
////
////
////// Create a Test Suite
////vows.describe('qrx Queue testing').addBatch({
////  'With a producer and consumer Queue':{
////    topic:{producer:new Queue('TEST'), consumer:new Queue('TEST')}, 
////      'With a producer and consumer Queue': {
////        topic: function (queues) {
////          console.log('test');
////          queues.producer.enqueue('Make me a sandwich.');
////          reactiveTopic(this.consumer.createWorkObservable(), this.callback);
////        }, 
////        'the consumer receives new workItem, when a workItem is enqueued': {
////          topic: function(queues, workItem){
////            assert.equal(workItem.work, 'Make me a sandwich.');
////            reactiveTopic(queues.producer.createCompletedObservable(), this.callback);
////            workItem.callback(null, 'Sandwich made!');
////          },
////          'the producer receives a completedWorkItem, with status complete':{
////            topic:function(queues, workItem, completedWorkItem) {
////              assert.equal(completedWorkItem.status, 'completed');
////              assert.equal(completedWorkItem.completedWork, 'Sandwich made!');
////            }
////          }
////        }
////      }
////  }
////});
////
