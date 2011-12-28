var assert = require('assert'),
    Rx = require('rx').Rx,
    uuid = require('node-uuid'),
    events = require('events');


var Channel  = require('../lib/channels.js').Channel;


exports.testChannelSndAndRcv = function(beforeExit, assert) {
  var testCh = new Channel('test');
  var receivedVal = null;
  var receivedClose = false;
  testCh.asObservable().Subscribe(function(r){
    receivedVal = r;
    testCh.close();
  },
  function(exn){
    
  },
  function(){
    receivedClose = true;
  });
  
  testCh.sendMessage('boo', function(e,o){});
  
  beforeExit(function(){
    assert.equal(receivedVal, 'boo')
    assert.equal(receivedClose, true)
  });
}


setTimeout(function(){process.exit(0)}, 2000);