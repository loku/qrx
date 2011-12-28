var redis = require('redis');
Rx = require('rx').Rx;

// Message Utility Functions

/**
 * Convert an array to a delimited string
 * @param  {String []}  fields
 * @param  {String} delimiter
 * @return {String} delimit string
 */
function implode(fields, delimiter) {
  var result;
  for (var i in fields) {
    var part = fields[i];
    result = result ? result + delimiter + part : part;
  } 
  return result;
}

/**
 * Creates a namespace string for redis objects
 */
function ns(parts){
  var args = Array.prototype.slice.call(arguments);
  return implode(args, '.');
} 

exports.ns = ns;

/**
 * Creates an observable redis channel
 * @param  {String} name channel name
 * @param  {Object} [redisOpts] redis connection options
 * @return {Rx.Observable}
 */
function createObservableChannel(name, redisOpts) {
  var ch = redisOpts ?
            redis.createClient(redisOpts.port, redisOpts.host, redisOpts):
            redis.createClient();
          
  return Rx.Observable.Create(function(obs){
    ch.on("message", function(channel, msg) {
      obs.OnNext(msg);
    });
    ch.subscribe(name);
    return function(){ch.end();};
  })
}

exports.createObservableChannel = createObservableChannel;


// Convenience functions for message pack/unpack
/**
 * Creates a workItem message
 * @param  {String}   prefix
 * @param  {Object}   msg message
 * @return {String}   work message
 */
function packMsg(prefix, msg){
  return prefix + '#' + JSON.stringify(msg);
}

/**
 * Unpacks a workItem from a message
 * @param {String} workMsg
 * @return {Object} workItem
 */
function unpackMsg(workMsg) {
  var fields = workMsg.split('#');
  var msg = JSON.parse(fields[1]);
  return msg;
}

var QRX_PREFIX = 'QRX';

function Channel(channelName, redisOptions){ 
  // messages
  // prefixing used to provide monitoring backplane using PMESSAGE
  // ToDo: The only reason this is required is no clone
  // method available on redisClient
  var client = redisOpts ? redis.createClient(redisOpts) : 
                           redis.createClient();
  
  var redisOpts = redisOptions;
  
  var ch = ns(QRX_PREFIX, channelName);
  
  this.redisObjects = {channel: ch};
  
  var MSG_PREFIX = ns(QRX_PREFIX, channelName);
  
  var CLOSE_MSG = 'close-' + ch;
  
  this.sendMessage = function(msg, callback){
    client.publish(ch, packMsg(MSG_PREFIX, msg), callback);
  }

  /**
   * Sends the close message on the channel signaling all
   * subscribers to channel that the channel is completed
   */
  this.close = function(){
    this.sendMessage(CLOSE_MSG, function(e,o){});
  }
  

  this.asObservable = function() {
    // subscribtion have to create their own
    // redis connection as the connection can
    // only be used for pub/sub when 'SUBSCRIBE'
    // is invoked
    var client = redisOpts ?
              redis.createClient(redisOpts.port, redisOpts.host, redisOpts):
              redis.createClient();

    return Rx.Observable.Create(function(obs){
      client.on("message", function(channel, msgStr) {
        var msg = unpackMsg(msgStr);
        if (msg == CLOSE_MSG){
          obs.OnCompleted();
        } else {
          obs.OnNext(msg);
        }
      });
      client.subscribe(ch);
      return function(){client.end();};
    });
  }
}

exports.Channel = Channel;
