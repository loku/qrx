var redis = require('redis'),
    uuid = require('node-uuid'),
    WorkItem = require('../lib/workitem').WorkItem,
    WorkSet = require('../lib/workset').WorkSet;

exports.testWorkSet = function(beforeExit, assert){
  var wi = new WorkItem('foo');
  var testSet = new WorkSet(uuid.v1());
  testSet.addToSet(wi, function(e,o){
    assert.isNull(e);
    testSet.setCount(function(e, count){
      assert.isNull(e);
      assert.equal(count,1);
      testSet.removeFromSet(wi.id, function(e,o){
        assert.isNull(e);
        testSet.setCount(function(e, count){
          assert.equal(count, 0);
          setTimeout(function(){process.exit(0)}, 0);
        })
      })
    })
  })
};

