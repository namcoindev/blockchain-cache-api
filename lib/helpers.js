// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const util = require('util')

class Helpers {
  static requestIp (request) {
    return request.header('x-forwarded-for') || request.ip
  }

  static requestUserAgent (request) {
    const agent = request.header('user-agent') || 'unknown'
    return agent.split(' ', 1).join(' ')
  }

  static toNumber (term) {
    if (typeof term === 'number') {
      return term
    }

    if (parseInt(term).toString() === term) {
      return parseInt(term)
    } else {
      return false
    }
  }

  static log (message) {
    log(message)
  }

  static logHTTPRequest (req, params, time) {
    params = params || ''
    if (!time && Array.isArray(params) && params.length === 2 && !isNaN(params[0]) && !isNaN(params[1])) {
      time = params
      params = ''
    }
    if (Array.isArray(time) && time.length === 2) {
      time = util.format('%s.%s', time[0], time[1])
      time = parseFloat(time)
      if (isNaN(time)) time = 0
      time = util.format(' [%ss]', time.toFixed(4).padStart(8, ' '))
    } else {
      time = ''
    }
    log(util.format('[REQUEST]%s [%s] (%s) %s %s', time, Helpers.requestIp(req).padStart(15, ' '), Helpers.requestUserAgent(req), req.path, params).green)
  }

  static logHTTPError (req, message, time) {
    if (Array.isArray(time) && time.length === 2) {
      time = util.format('%s.%s', time[0], time[1])
      time = parseFloat(time)
      if (isNaN(time)) time = 0
      time = util.format(' [%ss]', time.toFixed(4).padStart(8, ' '))
    } else {
      time = ''
    }
    message = message || 'Parsing error'
    log(util.format('[ERROR]%s [%s] (%s) %s: %s', time, Helpers.requestIp(req).padStart(15, ' '), Helpers.requestUserAgent(req), req.path, message).red)
  }
}

function log (message) {
  console.log(util.format('%s: %s', (new Date()).toUTCString(), message))
}

module.exports = Helpers
