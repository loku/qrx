var uuid = require('node-uuid');

/**
 * @constructor
 * Primative work datum with guaranteed unique id
 * @param {Object} work
 * @param {String} completedWorkQueue
 */
function WorkItem(work, completedWorkQueue){
  // generated work id
  this.id =  uuid.v1();
  this.work = work;

  // completed state
  this.status = null;
  this.completedWork = null;
  this.error = null;
  this.completedWorkQueue = completedWorkQueue;
}

exports.WorkItem = WorkItem;


/**
 * Maps a work item to a completed item
 * @param {WorkItem} workItem
 * @param {Object} err
 * @param {Object} completedWork
 * @return {WorkItem} New work item with completed status set
 */
function cloneCompleted(workItem,
                        err,
                        completedWork,
                        completedWorkQueue){

    var newWorkItem = new WorkItem(null);

    newWorkItem.id = workItem.id;
    newWorkItem.work = workItem.work;

    newWorkItem.status = err ? 'error' : 'completed'
    newWorkItem.completedWork = completedWork;
    newWorkItem.error = err;

    newWorkItem.completedWorkQueue = completedWorkQueue;

    return newWorkItem;
}
exports.cloneCompleted = cloneCompleted;
