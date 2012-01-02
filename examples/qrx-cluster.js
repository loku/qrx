var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length;


var forkmany = require('../lib/rx-extensions');
var WorkQueueRx  = require('../lib/qrx').WorkQueueRx;  



var workQueue = new WorkQueueRx('clustered-q');

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    console.log('forking');
    cluster.fork();
  }
} else {
  // Worker processes each have a queue
  workQueue.workObservable().Subscribe(function(workItem){
    workItem.callback(null, workItem.work + 10);
  })
}


if (cluster.isMaster){
  // enqueue some work for the cluster
  workQueue.completedObservable().Subscribe(function(workItem){
    console.log(workItem.completedWork);
  }, function(exn){
    console.log('work exception:', exn);
  },
  function(){
   console.log('Work Completed with with:', numCPUs, 'processors')
  });
  
  for (var i=0; i < 20; i++) {
    workQueue.enqueue(i);
  }
}