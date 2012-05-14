var redis = require('redis'),
    expect = require('chai').expect,
    sinon = require('sinon'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid'),
    Q = require('q');


var WorkItem = require('../lib/workitem').WorkItem,
    WorkSet = require('../lib/workset').WorkSet;

describe('WorkSet', function(){
  it('should be add, size the set and remove and item by id', function(){
    var wi = new WorkItem('foo');
    var testSet = new WorkSet(uuid.v1());
    Q.node(testSet.addToSet, testSet, wi)()
    .then(function(){
      return Q.node(testSet.size, testSet)();
    })
    .then(function(size){
      expect(size).to.equal(1);
      return Q.node(testSet.removeFromSet, testSet, wi.id)();
    })
    .then(function(){
      return Q.node(testSet.size, testSet)();
    })
    .then(function(size){
      expect(size).to.equal(0);
    });
  })
});