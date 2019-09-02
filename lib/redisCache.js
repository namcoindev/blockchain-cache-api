// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

const Crypto = require('crypto')
const inherits = require('util').inherits
const EventEmitter = require('events').EventEmitter
const redis = require('redis')

const RedisCache = function (opts) {
  opts = opts || {}
  if (!(this instanceof RedisCache)) return new RedisCache(opts)
  this.host = opts.host || '127.0.0.1'
  this.port = opts.port || 6379
  this.prefix = opts.prefix || false
  this.defaultTTL = opts.defaultTTL || 30

  this.client = redis.createClient({
    host: this.host,
    port: this.port,
    prefix: this.prefix
  })

  this.client.on('error', err => this.emit('error', err))
  this.client.on('ready', () => this.emit('ready'))
}
inherits(RedisCache, EventEmitter)

RedisCache.prototype.set = function (keyName, value, ttl) {
  return new Promise((resolve, reject) => {
    if (typeof value !== 'string') {
      value = JSON.stringify(value)
    }

    const key = sha256(keyName)
    this.client.set(key, value, 'EX', ttl || this.defaultTTL, (err, reply) => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

RedisCache.prototype.get = function (keyName) {
  return new Promise((resolve, reject) => {
    const key = sha256(keyName)
    this.client.get(key, (err, reply) => {
      if (err) return resolve(null)
      return resolve(JSON.parse(reply))
    })
  })
}

function sha256 (message) {
  if (typeof message !== 'string') {
    message = JSON.stringify(message)
  }
  return Crypto.createHmac('sha256', message).digest('hex')
}

module.exports = RedisCache
