var expect = require('chai').expect,
    sinon = require('sinon'),
    Q = require('q'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid');

var Channel  = require('../lib/channels.js').Channel;


describe('channels', function(){
  it('should be able to send messages and receive them by subscription', function(){
    var testChannel = new Channel(uuid.v1());
    var deferredMessage = Q.defer();
    var deferredClose = Q.defer();
    testChannel.asObservable().Subscribe(function(message) {
      expect(message).to.equal('foo');
      testChannel.close();
      deferredMessage.resolve(message);
    },
    function(exn){},
    function(){
      deferredClose.resolve(true);
    });
    return Q.all([Q.node(testChannel.sendMessage, testChannel, 'foo')(), 
                  deferredMessage.promise,
                  deferredClose.promise]);
  })
});

