var discovery = require('discovery-channel')
var pump = require('pump')
var events = require('events')
var util = require('util')
var net = require('net')
var toBuffer = require('to-buffer')
var crypto = require('crypto')
var lpmessage = require('length-prefixed-message')
var connections = require('connections')
var debug = require('debug')('simple-cloud-announcer')

try {
  var utp = require('utp-native')
} catch (err) {
  // do nothing
}

var HANDSHAKE_TIMEOUT = 5000

module.exports = SimpleCloudAnnouncer

function SimpleCloudAnnouncer (opts) {
  if (!(this instanceof SimpleCloudAnnouncer)) return new SimpleCloudAnnouncer(opts)
  if (!opts) opts = {}
  events.EventEmitter.call(this)

  var self = this

  this.totalConnections = 0

  this.connections = []
  this.id = opts.id || crypto.randomBytes(32)
  this.destroyed = false

  this._stream = opts.stream
  this._options = opts || {}
  this._discovery = null
  this._tcp = opts.tcp === false ? null : net.createServer().on('connection', onconnection)
  this._utp = opts.utp === false || !utp ? null : utp().on('connection', onconnection)
  this._tcpConnections = this._tcp && connections(this._tcp)
  this._listening = false

  function onconnection (connection) {
    var type = this === self._tcp ? 'tcp' : 'utp'
    var ip = connection.remoteAddress || connection.address().address
    var port = this.address().port
    debug('inbound connection type=%s ip=%s:%d', type, ip, port)
    connection.on('error', onerror)
    self._onconnection(connection, type, null)
  }
}

util.inherits(SimpleCloudAnnouncer, events.EventEmitter)

SimpleCloudAnnouncer.prototype.close =
SimpleCloudAnnouncer.prototype.destroy = function (onclose) {
  if (this.destroyed) return process.nextTick(onclose || noop)
  if (onclose) this.once('close', onclose)

  this.destroyed = true
  if (this._discovery) this._discovery.destroy()

  var self = this
  var missing = 0

  if (this._utp) {
    missing++
    for (var i = 0; i < this._utp.connections.length; i++) {
      this._utp.connections[i].destroy()
    }
  }

  if (this._tcp) {
    missing++
    this._tcpConnections.destroy()
  }

  if (this._listening) {
    if (this._tcp) this._tcp.close(onserverclose)
    if (this._utp) this._utp.close(onserverclose)
  } else {
    this.emit('close')
  }

  function onserverclose () {
    if (!--missing) self.emit('close')
  }
}

SimpleCloudAnnouncer.prototype.__defineGetter__('connecting', function () {
  return this.totalConnections - this.connections.length
})

SimpleCloudAnnouncer.prototype.__defineGetter__('connected', function () {
  return this.connections.length
})

SimpleCloudAnnouncer.prototype.join = function (name, opts, cb) {
  if (typeof opts === 'function') return this.join(name, {}, opts)
  name = toBuffer(name)
  if (!opts) opts = {}
  if (typeof opts.announce === 'undefined') opts.announce = true

  if (!this._listening) this.listen()

  var port
  if (opts.announce) port = this.address().port
  this._discovery.join(name, port, {impliedPort: opts.announce && !!this._utp}, cb)
}

SimpleCloudAnnouncer.prototype.leave = function (name) {
  name = toBuffer(name)
  this._discovery.leave(name, this.address().port)
}

SimpleCloudAnnouncer.prototype.address = function () {
  return this._tcp ? this._tcp.address() : this._utp.address()
}

SimpleCloudAnnouncer.prototype._onconnection = function (connection, type, peer) {
  var self = this

  var info = {
    type: type,
    initiator: !!peer,
    id: null,
    host: peer ? peer.host : connection.address().address,
    port: peer ? peer.port : connection.address().port,
    channel: peer ? peer.channel : null
  }

  this.totalConnections++
  connection.on('close', onclose)

  if (this._stream) {
    var wire = connection
    connection = this._stream(info)
    connection.on('handshake', onhandshake)
    if (this._options.connect) this._options.connect(connection, wire)
    else pump(wire, connection, wire)
  } else {
    handshake(connection, this.id, onhandshake)
  }

  var timeout = setTimeoutUnref(ontimeout, HANDSHAKE_TIMEOUT)
  if (this.destroyed) connection.destroy()

  function ontimeout () {
    connection.destroy()
  }

  function onclose () {
    clearTimeout(timeout)
    self.totalConnections--

    var i = self.connections.indexOf(connection)
    if (i > -1) {
      var last = self.connections.pop()
      if (last !== connection) self.connections[i] = last
    }
  }

  function onhandshake (remoteId) {
    if (!remoteId) remoteId = connection.remoteId
    clearTimeout(timeout)

    self.connections.push(connection)
    info.id = remoteId
    self.emit('connection', connection, info)
  }
}

SimpleCloudAnnouncer.prototype.listen = function (port, onlistening) {
  if (this.destroyed || this._listening) return
  if (this._tcp && this._utp) return this._listenBoth(port, onlistening)
  if (!port) port = 0
  if (onlistening) this.once('listening', onlistening)

  var self = this
  var server = this._tcp || this._utp

  // start listening
  this._listening = true
  server.on('error', onerror)
  server.on('listening', onlisten)
  server.listen(port)

  // announce on the discovery channel
  if (this._options.dns !== false) {
    if (!this._options.dns || this._options.dns === true) this._options.dns = {}
    this._options.dns.socket = this._utp
  }

  if (this._options.dht !== false) {
    if (!this._options.dht || this._options.dht === true) this._options.dht = {}
    this._options.dht.socket = this._utp
  }
  this._discovery = discovery(this._options)

  function onerror (err) {
    self.emit('error', err)
  }

  function onlisten () {
    self.emit('listening')
  }
}

SimpleCloudAnnouncer.prototype._listenBoth = function (port, onlistening) {
  if (typeof port === 'function') return this.listen(0, port)
  if (!port) port = 0
  if (onlistening) this.once('listening', onlistening)

  var self = this
  this._listening = true

  this._utp.on('error', onerror)
  this._utp.on('listening', onutplisten)
  this._tcp.on('listening', ontcplisten)
  this._tcp.on('error', onerror)
  this._tcp.listen(port)

  function cleanup () {
    self._utp.removeListener('error', onerror)
    self._tcp.removeListener('error', onerror)
    self._utp.removeListener('listening', onutplisten)
    self._tcp.removeListener('listening', ontcplisten)
  }

  function onerror (err) {
    cleanup()
    self._tcp.close(function () {
      if (!port) return self.listen() // retry
      self.emit('error', err)
    })
  }

  function onutplisten () {
    cleanup()
    self._utp.on('error', forward)
    self._tcp.on('error', forward)
    self.emit('listening')
  }

  function ontcplisten () {
    self._utp.listen(this.address().port)
  }

  function forward (err) {
    self.emit('error', err)
  }
}

function handshake (socket, id, cb) {
  lpmessage.write(socket, id)
  lpmessage.read(socket, cb)
}

function onerror () {
  this.destroy()
}

function setTimeoutUnref (fn, time) {
  var timeout = setTimeout(fn, time)
  if (timeout.unref) timeout.unref()
  return timeout
}

function noop () {}
