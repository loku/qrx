var redis = require('redis');
    Rx = require('rx').Rx,
    _ = require('underscore'),
    uuid = require('node-uuid');

newRedisClient = require('./redisutils').newRedisClient;

var Channel = module.exports = function() {
  Channel.CLOSE_MSG = 'QRX-channel-close';
  this.initialize.apply(this, arguments);
}

Channel.prototype.initialize = function(options) {
  options = options || {};
  _.defaults(options, {name: uuid.v1(),
                       redisOptions: null});
  _.extend(this, options);

  this.client = newRedisClient(this.redisOptions);
}

Channel.prototype.send = function(msg, callback) {
  if (typeof(msg) != 'string') {
    msg = JSON.stringify(msg);
  }

  if (callback == undefined) {
    callback = function(){};
  }
  this.client.publish(this.name, msg, callback);
}

/**
 * Closes channel, all subscribers to channel recv completed
 */
Channel.prototype.close = function() {
  this.send(Channel.CLOSE_MSG);
}

Channel.prototype.subscribe = function() {
  // create a new connection for the subscription
  // redis pub/sub blocks connection's in pub/sub mode
  var self = this;
  var rc = newRedisClient(this.redisOptions);
  
  var channelObs = Rx.Observable.Create(function(obs) {
    rc.on("message", function(chName, msg) {
      if (msg == Channel.CLOSE_MSG){
        obs.OnCompleted();
      } else {
        try {
          obs.OnNext(JSON.parse(msg));
        } catch(e) {
          obs.OnNext(msg);
        }

      }
    });
    rc.subscribe(self.name);
    return function(){rc.quit();};
  });

  // delegate subscription call
  channelObs.Subscribe.apply(channelObs, arguments);
}