var redis = require('redis');
Rx = require('rx').Rx;

// contsants
// Well known message to close a channel
var CLOSE_MSG = 'QRX-close';

/**
 * Creates an observable redis channel
 * @param  {String} ch channel name
 * @param  {Object} [redisOpts] redis connection options
 * @return {Rx.Observable}
 */
function createObservableChannel(ch, redisOpts) {
  // subscribtion have to create their own
    // redis connection as the connection can
    // only be used for pub/sub when 'SUBSCRIBE'
    // is invoked
    var client = redisOpts ?
              redis.createClient(redisOpts.port, redisOpts.host, redisOpts):
              redis.createClient();

    return Rx.Observable.Create(function(obs){
      client.on("message", function(channel, msg) {
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

exports.createObservableChannel = createObservableChannel;


// wrapper functions around redis PUB/SUB
/**
 * @param {redis.RedisClient} client
 * @param {String} ch channel name
 * @param {String} msg
 * @param {function} callback
 */
function sendMessage(client, ch, msg, callback){
    client.publish(ch, msg, callback);
}

exports.sendMessage = sendMessage;


function Channel(channelName, redisOptions){ 
  // messages
  // prefixing used to provide monitoring backplane using PMESSAGE
  // ToDo: The only reason this is required is no clone
  // method available on redisClient
  var client = redisOpts ? redis.createClient(redisOpts) : 
                           redis.createClient();
 
  var redisOpts = redisOptions;
  
  var ch = channelName;
  
  this.redisObjects = {channel: ch};
   
  this.sendMessage = function(msg, callback){
    sendMessage(client, ch, msg, callback);
  }

  /**
   * Sends the close message on the channel signaling all
   * subscribers to channel that the channel is completed
   */
  this.close = function(){
    this.sendMessage(CLOSE_MSG, function(e,o){});
  }
  
  /**
   * creates an observable based on the wrapped redis channel
   */
  this.asObservable = function() {
    return createObservableChannel(ch, redisOpts)
  }
}

exports.Channel = Channel;
