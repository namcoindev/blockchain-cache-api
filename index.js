// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

require('dotenv').config()
const BigInteger = require('big-integer')
const BodyParser = require('body-parser')
require('colors')
const Compression = require('compression')
const Config = require('./config.json')
const DatabaseBackend = require('./lib/databaseBackend.js')
const Express = require('express')
const Helmet = require('helmet')
const isHex = require('is-hex')
const RabbitMQ = require('amqplib')
const semver = require('semver')
const TurtleCoinNodeMonitor = require('turtlecoin-node-monitor')
const TurtleCoinPoolMonitor = require('turtlecoin-pool-monitor')
const TurtleCoinUtils = require('turtlecoin-utils').CryptoNote
const util = require('util')
const UUID = require('uuid/v4')

/* Load in our environment variables */
const env = {
  mysql: {
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || 3306,
    username: process.env.MYSQL_USERNAME || false,
    password: process.env.MYSQL_PASSWORD || false,
    database: process.env.MYSQL_DATABASE || false,
    connectionLimit: process.env.MYSQL_CONNECTION_LIMIT || 10
  },
  publicRabbit: {
    host: process.env.RABBIT_PUBLIC_SERVER || 'localhost',
    username: process.env.RABBIT_PUBLIC_USERNAME || '',
    password: process.env.RABBIT_PUBLIC_PASSWORD || ''
  }
}

/* Let's set up a standard logger. Sure it looks cheap but it's
   reliable and won't crash */
function log (message) {
  console.log(util.format('%s: %s', (new Date()).toUTCString(), message))
}

/* Sanity check to make sure we have connection information
   for the database */
if (!env.mysql.host || !env.mysql.port || !env.mysql.username || !env.mysql.password) {
  log('It looks like you did not export all of the required connection information into your environment variables before attempting to start the service.')
  process.exit(1)
}

/* Create an instance of the TurtleCoin Utils */
const coinUtils = new TurtleCoinUtils()

/* Helps us to build the RabbitMQ connection string */
function buildConnectionString (host, username, password) {
  log(util.format('Setting up connection to %s@%s...', username, host))
  var result = ['amqp://']

  if (username.length !== 0 && password.length !== 0) {
    result.push(username + ':')
    result.push(password + '@')
  }

  result.push(host)

  return result.join('')
}

var publicChannel
var replyQueue

async function connectRabbit () {
  /* Set up our access to the necessary RabbitMQ systems */
  var publicRabbit = await RabbitMQ.connect(buildConnectionString(env.publicRabbit.host, env.publicRabbit.username, env.publicRabbit.password))
  publicChannel = await publicRabbit.createChannel()

  publicRabbit.on('error', (error) => {
    log(util.format('[ERROR] %s', error))
    connectRabbit()
  })
  publicChannel.on('error', (error) => {
    log(util.format('[ERROR] %s', error))
    connectRabbit()
  })

  /* Set up the RabbitMQ queues */
  await publicChannel.assertQueue(Config.queues.relayAgent, {
    durable: true
  })

  /* Create our worker's reply queue */
  replyQueue = await publicChannel.assertQueue('', { exclusive: true, durable: false })
}

(async function () {
  await connectRabbit()
})()

function clientIp (req) {
  return req.header('x-forwarded-for') || req.ip
}

function logHTTPRequest (req, params, time) {
  params = params || ''
  if (!time && Array.isArray(params) && params.length === 2 && !isNaN(params[0]) && !isNaN(params[1])) {
    time = params
    params = ''
  }
  if (Array.isArray(time) && time.length === 2) {
    time = util.format(' [%s.%ss]', time[0], time[1])
  } else {
    time = ''
  }
  log(util.format('[REQUEST] (%s) %s %s%s', clientIp(req), req.path, params, time))
}

function logHTTPError (req, message, time) {
  if (Array.isArray(time) && time.length === 2) {
    time = util.format(' [%s.%ss]', time[0], time[1])
  } else {
    time = ''
  }
  message = message || 'Parsing error'
  log(util.format('[ERROR] (%s) %s: %s%s', clientIp(req), req.path, message, time).red)
}

/* This is a special magic function to make sure that when
   we parse a number that the whole thing is actually a
   number */
function toNumber (term) {
  if (typeof term === 'number') {
    return term
  }
  if (parseInt(term).toString() === term) {
    return parseInt(term)
  } else {
    return false
  }
}

/* Set up our database connection */
const database = new DatabaseBackend({
  host: env.mysql.host,
  port: env.mysql.port,
  username: env.mysql.username,
  password: env.mysql.password,
  database: env.mysql.database,
  connectionLimit: env.mysql.connectionLimit
})

log('Connected to database backend at ' + database.host + ':' + database.port)

const app = Express()

app.use((req, res, next) => {
  const ip = clientIp(req)
  if (Config.blacklistedIps.indexOf(ip) !== -1) {
    return res.status(403).send()
  }
  next()
})

/* Automatically decode JSON input from client requests */
app.use(BodyParser.json())

/* Catch body-parser errors */
app.use((err, req, res, next) => {
  if (err instanceof SyntaxError) {
    return res.status(400).send()
  }
  next()
})

/* Set up a few of our headers to make this API more functional */
app.use((req, res, next) => {
  res.header('X-Requested-With', '*')
  res.header('Access-Control-Allow-Origin', Config.corsHeader)
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
  res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
  res.header('Cache-Control', 'max-age=30, public')
  next()
})

/* Set up our system to use Helmet */
app.use(Helmet())

/* If we are configured to use compression in our config, we will activate it */
if (Config.useCompression) {
  app.use(Compression())
}

/* Return the underlying information about the daemon(s) we are polling */
app.get('/info', (req, res) => {
  const start = process.hrtime()
  database.getInfo().then((info) => {
    logHTTPRequest(req, process.hrtime(start))
    info.isCacheApi = true
    return res.json(info)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

app.get('/getinfo', (req, res) => {
  const start = process.hrtime()
  database.getInfo().then((info) => {
    logHTTPRequest(req, process.hrtime(start))
    info.isCacheApi = true
    return res.json(info)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get information regarding the current cache height */
app.get('/height', (req, res) => {
  const start = process.hrtime()
  var networkData
  database.getInfo().then((info) => {
    networkData = info
    return database.getLastBlockHeader()
  }).then((header) => {
    logHTTPRequest(req, process.hrtime(start))
    /* We shave one off the cached network_height as the underlying daemons
       misreport this information. The network_height indicates the block
       that the network is looking for, not the last block it found */
    return res.json({
      height: header.height,
      network_height: networkData.network_height - 1
    })
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get information regarding the current cache height */
app.get('/getheight', (req, res) => {
  const start = process.hrtime()
  var networkData
  database.getInfo().then((info) => {
    networkData = info
    return database.getLastBlockHeader()
  }).then((header) => {
    logHTTPRequest(req, process.hrtime(start))
    /* We shave one off the cached network_height as the underlying daemons
       misreport this information. The network_height indicates the block
       that the network is looking for, not the last block it found */
    return res.json({
      height: header.height,
      network_height: networkData.network_height - 1
    })
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get the current circulating currency amount */
app.get('/supply', (req, res) => {
  const start = process.hrtime()
  database.getLastBlockHeader().then((header) => {
    logHTTPRequest(req, process.hrtime(start))
    const supply = (header.alreadyGeneratedCoins / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()
    return res.send(supply)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Returns the latest 2,880 (1 day) block statistics to help
   better understand the state of the network */
app.get('/chain/stats', (req, res) => {
  const start = process.hrtime()
  database.getRecentChainStats().then((blocks) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(blocks)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Submit a new block to the network */
app.post('/block', (req, res) => {
  const start = process.hrtime()
  const blockBlob = req.body.block || false

  if (!blockBlob || !isHex(blockBlob)) {
    const message = 'Invalid block blob format'
    logHTTPError(req, message, process.hrtime(start))
    return res.status(400).json({ message: message })
  }

  var cancelTimer

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      if (response.error) {
        /* Log and spit back the response */
        logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
        return res.status(400).json({ message: response.error })
      } else {
        /* Log and spit back the response */
        logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.send(201).send()
      }
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const payload = {
    blockBlob: blockBlob
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(payload)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
    return res.status(500).send()
  }, 5500)
})

/* Get block information for the last 1,000 blocks before
   the specified block inclusive of the specified blocks */
app.get('/block/headers/:search/bulk', (req, res) => {
  const start = process.hrtime()
  const idx = toNumber(req.params.search) || -1

  /* If the caller did not specify a valid height then
     they most certainly didn't read the directions */
  if (idx === -1) {
    logHTTPError(req, 'No valid height provided', process.hrtime(start))
    return res.status(400).send()
  }

  database.getBlocks(idx, 1000).then((blocks) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(blocks)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get block information for the last 30 blocks before
   the specified block inclusive of the specified block */
app.get('/block/headers/:search', (req, res) => {
  const start = process.hrtime()
  const idx = toNumber(req.params.search) || -1

  /* If the caller did not specify a valid height then
     they most certainly didn't read the directions */
  if (idx === -1) {
    logHTTPError(req, 'No valid height provided', process.hrtime(start))
    return res.status(400).send()
  }

  database.getBlocks(idx).then((blocks) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(blocks)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get the last block header */
app.get('/block/header/top', (req, res) => {
  const start = process.hrtime()
  database.getLastBlockHeader().then((header) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(header)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get the block header for the specified block (by hash or height) */
app.get('/block/header/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* If we suspect that we were passed a hash, let's go look for it */
  if (idx.length === 64) {
    /* But first, did they pass us only hexadecimal characters ? */
    if (!isHex(idx)) {
      logHTTPError(req, 'Block hash is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlockHeaderByHash(idx).then((header) => {
      logHTTPRequest(req, process.hrtime(start))
      return res.json(header)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
  } else {
    /* If they didn't pass us a number, we need to get out of here */
    if (toNumber(idx) === false) {
      logHTTPError(req, 'Block height is not a number', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlockHeaderByHeight(idx).then((header) => {
      logHTTPRequest(req, process.hrtime(start))
      return res.json(header)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
  }
})

/* Get the count of blocks in the backend database */
app.get('/block/count', (req, res) => {
  const start = process.hrtime()
  database.getBlockCount().then((count) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json({
      blockCount: count
    })
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get a block template for mining */
app.post('/block/template', (req, res) => {
  const start = process.hrtime()
  const address = req.body.address || false
  const reserveSize = toNumber(req.body.reserveSize)

  /* If they didn't provide a reserve size then there's little we can do here */
  if (!reserveSize) {
    var error = 'Missing reserveSize value'
    logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  /* If the reserveSize is out of range, then throw an error */
  if (reserveSize < 0 || reserveSize > 255) {
    error = 'reserveSize out of range'
    logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  /* To get a block template, an address must be supplied */
  if (!address) {
    error = 'Missing address value'
    logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  try {
    coinUtils.decodeAddress(address)
  } catch (e) {
    error = 'Invalid address supplied'
    logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  var cancelTimer

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      if (response.error) {
        /* Log and spit back the response */
        logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
        return res.status(400).json({ message: response.error })
      } else {
        /* Log and spit back the response */
        logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json({
          blocktemplate: response.blocktemplate_blob,
          difficulty: response.difficulty,
          height: response.height,
          reservedOffset: response.reserved_offset
        })
      }
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const payload = {
    walletAddress: address,
    reserveSize: reserveSize
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(payload)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
    return res.status(500).send()
  }, 5500)
})

/* Get block information for the specified block (by hash or height) */
app.get('/block/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* If we suspect that we were passed a hash, let's go look for it */
  if (idx.length === 64) {
    /* But first, did they pass us only hexadecimal characters ? */
    if (!isHex(idx)) {
      logHTTPError(req, 'Block hash supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlock(idx).then((block) => {
      logHTTPRequest(req, process.hrtime(start))
      return res.json(block)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
  } else {
    /* If they didn't pass us a number, we need to get out of here */
    if (toNumber(idx) === false) {
      logHTTPError(req, 'Block height supplied is not a valid number', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlockHeaderByHeight(idx).then((header) => {
      return database.getBlock(header.hash)
    }).then((block) => {
      logHTTPRequest(req, process.hrtime(start))
      return res.json(block)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
  }
})

/* Get the current transaction pool */
app.get('/transaction/pool', (req, res) => {
  const start = process.hrtime()
  database.getTransactionPool().then((transactions) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(transactions)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get a transaction by its hash */
app.get('/transaction/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req, 'Transaction hash supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransaction(idx).then((transaction) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(transaction)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(404).send()
  })
})

/* Get transaction inputs by its hash */
app.get('/transaction/:search/inputs', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req, 'Transaction hash supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransactionInputs(idx).then((inputs) => {
    if (inputs.length === 0) {
      logHTTPRequest(req, process.hrtime(start))
      return res.status(404).send()
    }
    logHTTPRequest(req, process.hrtime(start))
    return res.json(inputs)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get transaction outputs by its hash */
app.get('/transaction/:search/outputs', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req, 'Transaction hash supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransactionOutputs(idx).then((outputs) => {
    if (outputs.length === 0) {
      logHTTPRequest(req, process.hrtime(start))
      return res.status(404).send()
    }
    logHTTPRequest(req, process.hrtime(start))
    return res.json(outputs)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Get all transactions hashes that have the supplied payment ID */
app.get('/transactions/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req, 'Payment ID supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransactionHashesByPaymentId(idx).then((hashes) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(hashes)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

app.get('/amounts', (req, res) => {
  const start = process.hrtime()
  database.getMixableAmounts(Config.defaultMixins).then((amounts) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json(amounts)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(404).send()
  })
})

/* Get random outputs for transaction mixing */
app.post('/randomOutputs', (req, res) => {
  const start = process.hrtime()
  const amounts = req.body.amounts || []
  const mixin = toNumber(req.body.mixin) || Config.defaultMixins

  /* If it's not an array then we didn't follow the directions */
  if (!Array.isArray(amounts)) {
    logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
    return res.status(400).send()
  }

  /* Check to make sure that we were passed numbers
     for each value in the array */
  for (var i = 0; i < amounts.length; i++) {
    var amount = toNumber(amounts[i])
    if (!amount) {
      logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
      return res.status(400).send()
    }
    amounts[i] = amount
  }

  /* Go and try to get our random outputs */
  database.getRandomOutputsForAmounts(amounts, mixin).then((randomOutputs) => {
    logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
    return res.json(randomOutputs)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Allow us to get just the information that a wallet needs to find
   the transactions that belong to the wallet */
app.post('/sync', (req, res) => {
  const start = process.hrtime()
  const lastKnownBlockHashes = req.body.lastKnownBlockHashes || []
  const blockCount = toNumber(req.body.blockCount) || 100
  const scanHeight = toNumber(req.body.scanHeight)

  /* If it's not an array then we didn't follow the directions */
  if (!Array.isArray(lastKnownBlockHashes) && !scanHeight) {
    logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
    return res.status(400).send()
  }

  if (!scanHeight) {
    var searchHashes = []
    /* We need to loop through these and validate that we were
     given valid data to search through and not data that does
     not make any sense */
    lastKnownBlockHashes.forEach((elem) => {
    /* We need to check to make sure that they sent us 64 hexadecimal characters */
      if (elem.length === 64 && isHex(elem)) {
        searchHashes.push(elem)
      }
    })

    /* If, after sanitizing our input, we don't have any hashes
     to search for, then we're going to stop right here and
     say something about it */
    if (searchHashes.length === 0) {
      logHTTPError(req, 'No search hashes supplied', process.hrtime(start))
      return res.status(400).send()
    }

    database.getWalletSyncData(searchHashes, blockCount).then((outputs) => {
      req.body.lastKnownBlockHashes = req.body.lastKnownBlockHashes.length
      logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
      return res.json(outputs)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
  } else {
    database.getWalletSyncDataByHeight(scanHeight, blockCount).then((outputs) => {
      logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
      return res.json(outputs)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
  }
})

/* Allows us to provide a method to send a raw transaction on the network
   endpoint that works with our blockchain relay agent workers */
app.post('/transaction', (req, res) => {
  const start = process.hrtime()
  const transaction = req.body.tx_as_hex || false
  var cancelTimer

  /* If there is no transaction or the data isn't hex... we're done here */
  if (!transaction || !isHex(transaction)) {
    logHTTPError(req, 'Invalid or no transaction hex data supplied', process.hrtime(start))
    return res.status(400).send()
  }

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      /* Log and spit back the response */
      logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
      return res.json(response)
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const tx = {
    rawTransaction: transaction,
    hash: coinUtils.cnFastHash(transaction)
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(tx)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
    return res.status(500).send()
  }, 5500)
})

/* Legacy daemon API calls provided for limited support */

app.get('/fee', (req, res) => {
  const start = process.hrtime()
  logHTTPRequest(req, process.hrtime(start))
  return res.json({
    address: Config.nodeFee.address,
    amount: Config.nodeFee.amount,
    status: 'OK'
  })
})

app.get('/feeinfo', (req, res) => {
  const start = process.hrtime()
  logHTTPRequest(req, process.hrtime(start))
  return res.json({
    address: Config.nodeFee.address,
    amount: Config.nodeFee.amount,
    status: 'OK'
  })
})

app.post('/getwalletsyncdata/preflight', (req, res) => {
  const start = process.hrtime()
  const startHeight = toNumber(req.body.startHeight)
  const startTimestamp = toNumber(req.body.startTimestamp)
  const blockHashCheckpoints = req.body.blockHashCheckpoints || []

  blockHashCheckpoints.forEach((checkpoint) => {
    /* If any of the supplied block hashes aren't hexadecimal then we're done */
    if (!isHex(checkpoint)) {
      logHTTPError(req, 'Block hash supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }
  })

  /* We cannot supply both values */
  if (startHeight > 0 && startTimestamp > 0) {
    logHTTPError(req, 'Cannot supply both startHeight and startTimestamp', process.hrtime(start))
    return res.status(400).send()
  }

  database.legacyGetWalletSyncDataPreflight(startHeight, startTimestamp, blockHashCheckpoints).then((syncData) => {
    req.body.blockHashCheckpoints = blockHashCheckpoints.length
    logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
    return res.json({ height: syncData.height, blockCount: syncData.blockCount, status: 'OK' })
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

app.post('/getwalletsyncdata', (req, res) => {
  const start = process.hrtime()
  const startHeight = toNumber(req.body.startHeight)
  const startTimestamp = toNumber(req.body.startTimestamp)
  const blockHashCheckpoints = req.body.blockHashCheckpoints || []
  const blockCount = toNumber(req.body.blockCount) || 100
  const skipCoinbaseTransactions = (req.body.skipCoinbaseTransactions)

  blockHashCheckpoints.forEach((checkpoint) => {
    /* If any of the supplied block hashes aren't hexadecimal then we're done */
    if (!isHex(checkpoint)) {
      logHTTPError(req, 'Block hash supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }
  })

  /* We cannot supply both values */
  if (startHeight > 0 && startTimestamp > 0) {
    logHTTPError(req, 'Cannot supply both startHeight and startTimestamp', process.hrtime(start))
    return res.status(400).send()
  }

  database.legacyGetWalletSyncData(startHeight, startTimestamp, blockHashCheckpoints, blockCount, skipCoinbaseTransactions).then((results) => {
    req.body.blockHashCheckpoints = blockHashCheckpoints.length

    if (results.length >= 1) {
      req.body.range = {
        start: results[0].blockHeight,
        end: results[results.length - 1].blockHeight
      }
    }

    if (results.length !== 0) {
      logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
      return res.json({
        items: results,
        status: 'OK',
        synced: false
      })
    } else {
      database.getLastBlockHeader().then((block) => {
        logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json({
          items: results,
          status: 'OK',
          synced: true,
          topBlock: {
            height: block.height,
            hash: block.hash
          }
        })
      })
    }
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

app.get('/getwalletsyncdata/:height/:count', (req, res) => {
  const start = process.hrtime()
  const startHeight = toNumber(req.params.height)
  const blockCount = toNumber(req.params.count) || 100

  database.legacyGetWalletSyncDataLite(startHeight, blockCount).then((results) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json({ items: results, status: 'OK' })
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

app.get('/getwalletsyncdata/:height', (req, res) => {
  const start = process.hrtime()
  const startHeight = toNumber(req.params.height)
  const blockCount = toNumber(req.params.count) || 100

  database.legacyGetWalletSyncDataLite(startHeight, blockCount).then((results) => {
    logHTTPRequest(req, process.hrtime(start))
    return res.json({ items: results, status: 'OK' })
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

app.post('/get_transactions_status', (req, res) => {
  const start = process.hrtime()
  const transactionHashes = req.body.transactionHashes || []

  transactionHashes.forEach((hash) => {
    if (!isHex(hash)) {
      logHTTPError(req, 'Transaction has supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }
  })

  database.getTransactionsStatus(transactionHashes).then((result) => {
    logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
    return res.json(result)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Allows us to provide a daemon like /sendrawtransaction
   endpoint that works with our blockchain relay agent workers */
app.post('/sendrawtransaction', (req, res) => {
  const start = process.hrtime()
  const transaction = req.body.tx_as_hex || false
  var cancelTimer

  /* If there is no transaction or the data isn't hex... we're done here */
  if (!transaction || !isHex(transaction)) {
    logHTTPError(req, 'Invalid or no transaction hex data supplied', process.hrtime(start))
    return res.status(400).send()
  }

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      /* Log and spit back the response */
      logHTTPRequest(req, JSON.stringify(req.body) + '[' + response.status.yellow + ']', process.hrtime(start))
      return res.json(response)
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const tx = {
    rawTransaction: transaction,
    hash: coinUtils.cnFastHash(transaction)
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(tx)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
    return res.status(500).send()
  }, 5500)
})

/* Returns the last block reward */
app.get('/reward/last', (req, res) => {
  const start = process.hrtime()
  database.getLastBlockHeader().then((header) => {
    logHTTPRequest(req, process.hrtime(start))

    const reward = (header.baseReward / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()

    return res.send(reward)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Returns the next block reward */
app.get('/reward/next', (req, res) => {
  const start = process.hrtime()
  database.getLastBlockHeader().then((header) => {
    logHTTPRequest(req, process.hrtime(start))

    const reward = BigInteger(Config.maxSupply)
      .subtract(header.alreadyGeneratedCoins)
      .shiftRight(Config.emissionSpeed)
      .toJSNumber()

    const nextReward = (reward / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()

    return res.send(nextReward)
  }).catch((error) => {
    logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  })
})

/* Basic status response via GET that responds to basic monitoring requests */
app.get('/status', (req, res) => {
  return res.json({ status: 'ok' })
})

/* These API methods are only available if we have been
   configured as having access to node monitor data in the
   same database */
if (Config.useNodeMonitor) {
/* Set up our access to the node monitor information */
  const nodes = new TurtleCoinNodeMonitor({
    host: env.mysql.host,
    port: env.mysql.port,
    username: env.mysql.username,
    password: env.mysql.password,
    database: env.mysql.database,
    connectionLimit: env.mysql.connectionLimit
  })

  app.get('/node/list', (req, res) => {
    const start = process.hrtime()
    const maxFee = toNumber(req.query.max_fee) || false
    var minVersion = req.query.min_version || false

    if (minVersion) {
      minVersion = semver.clean(minVersion)
      if (!semver.valid(minVersion)) minVersion = false
    }

    nodes.getNodeStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = {
        nodes: []
      }

      stats.forEach((node) => {
        if (maxFee && node.feeAmount >= maxFee) return
        node.version = semver.clean(node.version)
        if (!semver.valid(node.version)) return
        if (minVersion && semver.lt(node.version, minVersion)) return

        response.nodes.push({
          name: node.name,
          url: node.hostname,
          port: node.port,
          ssl: (node.ssl === 1),
          cache: (node.cache === 1),
          fee: {
            address: node.feeAddress,
            amount: node.feeAmount
          },
          availability: node.availability,
          online: node.status,
          version: node.version,
          timestamp: node.lastCheckTimestamp
        })
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })

  app.get('/node/list/online', (req, res) => {
    const start = process.hrtime()
    const maxFee = toNumber(req.query.max_fee) || false
    var minVersion = req.query.min_version || false

    if (minVersion) {
      minVersion = semver.clean(minVersion)
      if (!semver.valid(minVersion)) minVersion = false
    }

    nodes.getNodeStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = {
        nodes: []
      }

      stats.forEach((node) => {
        if (!node.status) return
        if (maxFee && node.feeAmount >= maxFee) return
        node.version = semver.clean(node.version)
        if (!semver.valid(node.version)) return
        if (minVersion && semver.lt(node.version, minVersion)) return

        response.nodes.push({
          name: node.name,
          url: node.hostname,
          port: node.port,
          ssl: (node.ssl === 1),
          cache: (node.cache === 1),
          fee: {
            address: node.feeAddress,
            amount: node.feeAmount
          },
          availability: node.availability,
          online: node.status,
          version: node.version,
          timestamp: node.lastCheckTimestamp
        })
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })

  app.get('/node/list/available', (req, res) => {
    const start = process.hrtime()
    const maxFee = toNumber(req.query.max_fee) || false
    var minVersion = req.query.min_version || false

    if (minVersion) {
      minVersion = semver.clean(minVersion)
      if (!semver.valid(minVersion)) minVersion = false
    }

    nodes.getNodeStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = {
        nodes: []
      }

      stats.forEach((node) => {
        if (node.availability === 0) return
        if (maxFee && node.feeAmount >= maxFee) return
        node.version = semver.clean(node.version)
        if (!semver.valid(node.version)) return
        if (minVersion && semver.lt(node.version, minVersion)) return

        response.nodes.push({
          name: node.name,
          url: node.hostname,
          port: node.port,
          ssl: (node.ssl === 1),
          cache: (node.cache === 1),
          fee: {
            address: node.feeAddress,
            amount: node.feeAmount
          },
          availability: node.availability,
          online: node.status,
          version: node.version,
          timestamp: node.lastCheckTimestamp
        })
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })

  app.get('/node/stats', (req, res) => {
    const start = process.hrtime()

    nodes.getNodeStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = []

      stats.forEach((node) => {
        if (node.availability === 0) return

        const obj = {
          name: node.name,
          url: node.hostname,
          port: node.port,
          ssl: (node.ssl === 1),
          cache: (node.cache === 1),
          fee: {
            address: node.feeAddress,
            amount: node.feeAmount
          },
          availability: node.availability,
          online: node.status,
          version: node.version,
          timestamp: node.lastCheckTimestamp,
          height: node.height,
          connectionsIn: node.connectionsIn,
          connectionsOut: node.connectionsOut,
          history: []
        }

        node.history.forEach((evt) => {
          obj.history.push({
            timestamp: evt.timestamp,
            online: evt.status
          })
        })

        obj.history.sort((a, b) => (a.timestamp < b.timestamp) ? 1 : -1)

        response.push(obj)
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })
}

/* These API methods are only available if we have been
   configured as having access to pool monitor data in the
   same database */
if (Config.useNodeMonitor) {
/* Set up our access to the node monitor information */
  const pools = new TurtleCoinPoolMonitor({
    host: env.mysql.host,
    port: env.mysql.port,
    username: env.mysql.username,
    password: env.mysql.password,
    database: env.mysql.database,
    connectionLimit: env.mysql.connectionLimit
  })

  app.get('/pool/list', (req, res) => {
    const start = process.hrtime()

    pools.getPoolStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = {
        pools: []
      }

      stats.forEach((pool) => {
        response.pools.push({
          name: pool.name,
          url: pool.url,
          api: pool.api,
          type: pool.type,
          miningAddress: pool.miningAddress,
          mergedMining: (pool.mergedMining === 1),
          mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
          fee: pool.fee,
          minPayout: pool.minPayout,
          timestamp: pool.lastCheckTimestamp,
          availability: pool.availability,
          online: pool.status
        })
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })

  app.get('/pool/list/online', (req, res) => {
    const start = process.hrtime()

    pools.getPoolStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = {
        pools: []
      }

      stats.forEach((pool) => {
        if (!pool.status) return

        response.pools.push({
          name: pool.name,
          url: pool.url,
          api: pool.api,
          type: pool.type,
          miningAddress: pool.miningAddress,
          mergedMining: (pool.mergedMining === 1),
          mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
          fee: pool.fee,
          minPayout: pool.minPayout,
          timestamp: pool.lastCheckTimestamp,
          availability: pool.availability,
          online: pool.status
        })
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })

  app.get('/pool/list/available', (req, res) => {
    const start = process.hrtime()

    pools.getPoolStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = {
        pools: []
      }

      stats.forEach((pool) => {
        if (!pool.availability || pool.availability === 0) return

        response.pools.push({
          name: pool.name,
          url: pool.url,
          api: pool.api,
          type: pool.type,
          miningAddress: pool.miningAddress,
          mergedMining: (pool.mergedMining === 1),
          mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
          fee: pool.fee,
          minPayout: pool.minPayout,
          timestamp: pool.lastCheckTimestamp,
          availability: pool.availability,
          online: pool.status
        })
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })

  app.get('/pool/stats', (req, res) => {
    const start = process.hrtime()

    pools.getPoolStats().then((stats) => {
      logHTTPRequest(req, process.hrtime(start))

      const response = []

      stats.forEach((pool) => {
        if (!pool.availability || pool.availability === 0) return

        const obj = {
          name: pool.name,
          url: pool.url,
          api: pool.api,
          type: pool.type,
          miningAddress: pool.miningAddress,
          mergedMining: (pool.mergedMining === 1),
          mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
          fee: pool.fee,
          minPayout: pool.minPayout,
          timestamp: pool.lastCheckTimestamp,
          availability: pool.availability,
          online: pool.status,
          height: pool.height,
          hashrate: pool.hashrate,
          miners: pool.miners,
          lastBlock: pool.lastBlock,
          donation: pool.donation,
          history: []
        }

        pool.history.forEach((evt) => {
          obj.history.push({
            timestamp: evt.timestamp,
            online: evt.status
          })
        })

        obj.history.sort((a, b) => (a.timestamp < b.timestamp) ? 1 : -1)

        response.push(obj)
      })

      return res.json(response)
    }).catch((error) => {
      logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
  })
}

/* Response to options requests for preflights */
app.options('*', (req, res) => {
  return res.status(200).send()
})

/* This is our catch all to return a 404-error */
app.all('*', (req, res) => {
  logHTTPError(req, 'Requested URL not Found (404)')
  return res.status(404).send()
})

app.listen(Config.httpPort, Config.bindIp, () => {
  log('HTTP server started on ' + Config.bindIp + ':' + Config.httpPort)
})
