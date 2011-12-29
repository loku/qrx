// Message Utility Functions

var QRX_PREFIX = 'QRX';

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

exports.packMsg = packMsg;

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
exports.unpackMsg = unpackMsg;

function qrxChannelName(qname, channelName){
  return ns(QRX_PREFIX, qname, channelName);
}

exports.qrxChannelName = qrxChannelName;
 