```
   __ _  _ __ __  __
  / _` || '__|\ \/ /
 | (_| || |    >  < 
  \__, ||_|   /_/\_\
     | |            
     |_|            
                         
```

*A distributed work queue for node.js based on redis and RxJS.*

## Overview
'qrx' provides reactive work queue implemented with redis and RxJS. The
goals of the project:

* Enable shared asynchronous compute resources on top a native node.js technology stack.
* Focus on minimalism
* Ease of installation (redis and on node are easy)
* Practical performance: Approach near raw redis performance levels with queuing primitives

## Installation

npm install qrx

### Prerequisites
`qrx` requires a redis installation see: http://redis.io/download

### npm install
[sudo] npm install qrx

## Features
* Create a queue with name
* Enable async compute resource 

## Usage
*(From: /examples/hello-qrx.js)*

Allows you to crawl to a depth of *n* into a website

```javascript
// create a new queue with well known name
wq = new WorkQueueRx('test-wq');
// clear any pending work (optional)
wq.clear();

wq.enqueue('one');
wq.enqueue('two');

// subscribe for work
wq.workObservable().Subscribe(function(workObj){
  console.log('new work', workObj.work);
  // callback to mark the work completed or err'd
  workObj.callback(null, workObj.work + ' - completed');
});

// subscribe for completed work
wq.completedObservable().Subscribe(function(completedWork){
  console.log('completed work', completedWork)
})
```


## Credits

## ToDo
* Work stop singals
* Enable transactional queueing using redis primatives
* Performance optimization
* Flood control on queue restarts

## License ##

    Copyright (c) Loku. All rights reserved. The use and
    distribution terms for this software are covered by the Eclipse
    Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
    which can be found in the file epl-v10.html at the root of this
    distribution. By using this software in any fashion, you are
    agreeing to be bound by the terms of this license. You must
    not remove this notice, or any other, from this software.
