// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

const Crypto = require('crypto')
const EventEmitter = require('events').EventEmitter
const redis = require('redis')

class RedisCache extends EventEmitter {
  constructor (opts) {
    super()

    opts = opts || {}

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

  get (keyName) {
    return new Promise((resolve, reject) => {
      const key = sha256(keyName)
      this.client.get(key, (err, reply) => {
        if (err) return resolve(false)
        return resolve(JSON.parse(reply))
      })
    })
  }

  set (keyName, value, ttl) {
    return new Promise((resolve, reject) => {
      if (!value) {
        return reject(new Error('Cannot save value that does not exist'))
      }

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

  setNoExpire (keyName, value) {
    return new Promise((resolve, reject) => {
      if (!value) {
        return reject(new Error('Cannot save value that does not exist'))
      }

      if (typeof value !== 'string') {
        value = JSON.stringify(value)
      }

      const key = sha256(keyName)
      this.client.set(key, value, (err, reply) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }

  setTtl (keyName, ttl) {
    return new Promise((resolve, reject) => {
      const key = sha256(keyName)
      this.client.expire(key, ttl, (err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }
}

function sha256 (message) {
  if (typeof message !== 'string') {
    message = JSON.stringify(message)
  }
  return Crypto.createHmac('sha256', message).digest('hex')
}

module.exports = RedisCache
