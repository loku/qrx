var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length;


var forkmany = require('../lib/rx-extensions');
var Qrx  = require('../lib/qrx');  



var workQueue = new Qrx({qname:'test-cluster'});

var workCount = 100;

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
  
  var completedCount = 0;
  workQueue.completedObservable().Subscribe(function(workItem){
    completedCount++;
    
    if (completedCount == workCount) {
      workQueue.stop();
    }
  }, function(exn){
    console.log('work exception:', exn);
  },
  function(){
   console.log('Work Completed with with:', numCPUs, 'processors')
  });
  
  for (var i=0; i < workCount; i++) {
    workQueue.enqueue(i);
  }
}