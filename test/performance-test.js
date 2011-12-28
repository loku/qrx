redis = require('redis');


var work  = require('../lib/work.js'),
    WorkItem = work.WorkItem,
    WorkQueue = work.WorkQueue,
    WorkSet = work.WorkSet;
    
var Channel  = require('../lib/channels.js').Channel;

var WorkQueueRx  = require('../lib/qrx.js').WorkQueueRx;   

client = redis.createClient();
console.log('Raw redis queueing with list.')
start = Date.now()
for(var i = 0; i < 100000; i++) {
  client.rpush('test-p', 'foo');
}
console.log('Total time:', Date.now() - start + " ms");

console.log('WorkQueue: Redis list wrapper for queueing')
start = Date.now()
q = new WorkQueue('test-p', client);

for(i = 0; i < 100000; i++) {
  q.enqueue('test-p-foo', function(e,o){});
}
console.log('Total time:', Date.now() - start + " ms");

console.log('Per test for JSON.stringify 100k objects')
start = Date.now()
for(i = 0; i < 100000; i++) {
  var s = JSON.stringify({foo:'bar', bing:'baz'});
  var o = JSON.parse(s);
}
console.log('Total time:', Date.now() - start + " ms");

start = Date.now()
console.log('Channel performance test 100k messages.')
ch = new Channel('test-p-foo');
var msgCount = 0;
ch.asObservable().Subscribe(function(r){
  msgCount++;
})
for(i = 0; i < 100000; i++) {
  ch.sendMessage('foo', function(e,o){});
}
console.log('Total time:', Date.now() - start + " ms");


var wq = new WorkQueueRx('test-p-wq1')
wq.clear();
start = Date.now()
var WORK_COUNT = 10000;
console.log('WorkQueueRx performance test - enqueue' + WORK_COUNT + ' work items.')

var msgCount = 0;
var WORK_COUNT = 10000;

for(i = 0; i < WORK_COUNT; i++) {
  wq.enqueue('foo');
}
console.log('Total time:', Date.now() - start + " ms");

console.log('WorkQueueRx performance test - consuming' + WORK_COUNT + ' work items.')
var workItems = 1;
wq.workObservable().Subscribe(function(workItem){
  if (workItems == 1){
    start = Date.now();
  }
  workItem.callback(null, workItem.work + 'completed');
  if (workItems++ == WORK_COUNT) {
    console.log('Completed items:' + (workItems-1));
    console.log('Total time:', Date.now() - start + " ms");
  }
},
function(exn){},
function() {
});


