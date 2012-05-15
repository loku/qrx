var uuid = require('node-uuid'),
    _ = require('underscore');


WorkItem  = module.exports =  function() {
  this.initialize.apply(this, arguments);
}

WorkItem.prototype.initialize = function(options) {
  options = options || {};
  if (options.work == undefined) {
    throw 'WorkItem work undefined'
  }
  // work meta data
  this.id = uuid.v1();
  this.work = options.work;
  this.completedWorkQueue = options.completedWorkQueue;
  //console.log('completedWorkQueue', this.completedWorkQueue);
  this.queuedTime = Date.now();
  this.startTime = 0;
  this.status = 'pending';
}

WorkItem.markCompleted = function(workItemJSON, error, completedWork) {
  if (!completedWork && !error) {
    throw new Error('completedWork OR error needs to be defined');
  }
  // normalized status
  workItemJSON.status = error ? 'error' : 'completed';
  workItemJSON.completedWork = completedWork;
  workItemJSON.error = error;
  workItemJSON.endTime = Date.now();
}