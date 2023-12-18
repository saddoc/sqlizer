#!/bin/bash

':' //; export NODE_OPTIONS=--experimental-repl-await;
':' //; export NODE_PATH=$(npm root -gq):$(npm root -q):.
':' //; [ $# -le 0 ] && exec "$(which node)" -r $0 || exec "$(which node)" -r $0 \$0 $*

// rdb.js
//

const crypto = require('crypto')
const fs = require('fs');
const rocksdb = require('rocksdb')

async function rocksdb_open(fn, opts) {
    var resolve, reject;
    var o = new Promise((_resolve, _reject) => {
        resolve = _resolve
        reject = _reject
    })
    var db = rocksdb(fn)

    db.put2 = async function(key, value) {
        return new Promise((resolve, reject) => {
            this.put(key, value, (err) => {
                if (err != null)
                    reject(err)
                else
                    resolve()
            })
        })
    }
    db.get2 = async function(key) {
        return new Promise((resolve, reject) => {
            this.get(key, (value, err) => {
                if (err != null)
                    reject(err)
                else
                    resolve(value)
            })
        })
    }
    db.iterator2 = async function(opts) {
        var it = db.iterator(opts)

        it.next2 = function() {
            return new Promise((resolve, reject) => {
                this.next((err, key, value) => {
                    if (err != null)
                        reject(err)
                    else {
                        resolve([ key, value ])
                    }
                })
            })
        }
        it.end2 = function() {
            return new Promise((resolve, reject) => {
                this.end((err) => {
                    if (err != null)
                        reject(err)
                    else {
                        resolve()
                    }
                })
            })
        }

        return it
    }

    /*
     * var batch = db.batch()
     * await batch.put(key, value).put(key, value)
     * await db.batchWrite(batch)
     */
    db.batchWrite = async function(batch) {
        return new Promise((resolve, reject) => {
            batch.write((err) => {
                if (err != null)
                    reject(err)
                else
                    resolve()
            })
        })
    }

    db.open(opts, (err) => {
        if (err != null) {
            reject(err)
        } else {
            resolve(db)
        }
    })
    return o
}


async function batch_example() {
  // Create a batch operation
  const batch = db.batch();

  batch.put('name', 'John Doe')
       .put('age', '30')
       .put('city', 'New York')
       .write((err) => {
         if (err) {
           console.error('Failed to write batch:', err);
         } else {
           console.log('Batch write successful!');
         }
       });
}

async function rrr() {
    var db;
    try {
        db = await rocksdb_open("xxx.db", {
            createIfMissing: true, 
            cacheSize: 1024 * 1024 * 1024,
            maxFileSize: 64 * 1024 * 1024
        })
    } catch (e) {
        console.log("xxx", e)
        return
    }

    await db.put2("txhist.key.a1", "ABC")
    await db.put2("txhist.key.a2", "DEF")
    await db.put2("txhist.key.a3", "GHI")
    await db.put2("txhist.key.a4", "JKL")

    await db.get("txhist.key.a1", (value, err) => {
        console.log("xxx", value, err)
    })

    var it = await db.iterator2({
        gte: "txhist.key.",
        lt: "txhist.key/",
        keys: true,
        values: true,
        keysAsBuffer: true,
        valuesAsBuffer: false,

        limit: 11111000
    })
    for await (const [key, value] of it) {
        console.log("xxx", key.toString(), value.toString())
    }

    var it = await db.iterator2({
        gte: "txhist.key.a2",
        // lte: "txhist.key.a4",
        keys: true,
        values: true,
        keysAsBuffer: false,
        valuesAsBuffer: false,

        limit: 11111000
    })
    while (true) {
        const [ key, value ] = await it.next2()
        if (!key && !value)
            break
        console.log("ooo", key.toString(), value.toString())
    }
    await it.end2()

    await db.close((err) => {
        console.log("close", err)
    })
}

function pad(str, size) {
    return str.padStart(size, ' ')
}

function toIndexKey(type, token, user, id) {
    return `txhistory.ix1.${pad(type, 16)}.${pad(token, 66)}.${pad(user, 66)}.${pad(id.toString(16), 32)}`
}

async function load_data() {
    var data = require('fs').readFileSync("ooo.data").toString()
    data = JSON.parse(data)

    var db;
    try {
        db = await rocksdb_open("xxx.db", {
            createIfMissing: true, 
            cacheSize: 1024 * 1024 * 1024,
            maxFileSize: 64 * 1024 * 1024
        })
    } catch (e) {
        console.log("xxx", e)
        return
    }

    var xxx;
    for (var i of data) {
        db.put2(`txhistory.tx_hash.${i.tx_hash}`, JSON.stringify(i))
        // type, token0, user, id
        xxx = db.put2(toIndexKey(i.type, i.token0, i.user, i.id), JSON.stringify(i))
    }
    await xxx
    db.close((err) => {})
}

opendb = async function(name) {
    var db;
    try {
        db = await rocksdb_open(name, {
            createIfMissing: true, 
            cacheSize: 1024 * 1024 * 1024,
            maxFileSize: 64 * 1024 * 1024
        })
    } catch (e) {
        console.log(`failed to open ${name}: ${e}`)
        return
    }
    return db
}

// type = SWAP | POOL
query = async function(db, type, which) {
    const process = require('process')

    var st = process.hrtime()
    var six = `txhistory.ix1.${pad(type, 16)}.`
    var eix = `txhistory.ix1.${pad(type, 16)}/`
    var it = await db.iterator2({
        gte: six,
        lt: eix,
        keys: false,
        values: false,
        keysAsBuffer: false,
        valuesAsBuffer: false,
        // fillCache: true,

        limit: 11111000
    })

    var n = 0;
    if (!which || which == 0) {
        for await (const [key, value] of it) {
            n++
        }
    } else {
        while (true) {
            const [ key, value ] = await it.next2()
            if (!key && !value)
                break
            // console.log("xxx", key, value)
            n++
        }
        await it.end2()
    }
    var et = process.hrtime(st)
    var dtms = et[0] * 1000 + et[1] / 1000000

    console.log("xxx", n, dtms)
}

xxx = function(count) {
    const process = require('process')
    var st = process.hrtime()
    var n = 0;
    for (var i = 0; i < count; i++) {
        n++;
    }
    var et = process.hrtime(st)
    var dtms = et[0] * 1000 + et[1] / 1000000

    console.log("xxx", n, dtms)
}

async function sha256(x) {
    return '0x' + crypto.createHash('sha256').update(x).digest('hex')
}

// Function to get stats for a specified device
function getDiskIOStatsForDevice(device) {
    const content = fs.readFileSync('/proc/diskstats', 'utf8');
    const lines = content.split('\n');
    for (let line of lines) {
        if (line.includes(device)) {
            const fields = line.trim().split(/\s+/);
            return {
                reads: parseInt(fields[3]),
                writes: parseInt(fields[7]),
                readBytes: parseInt(fields[5]) * 512,
                writeBytes: parseInt(fields[9]) * 512,
            };
        }
    }
    return null;
}

// 2023-03-01 00:07:18
function toDateString(x) {
    if (!(x instanceof Date))
        x = new Date(x)
    function pad(v) {
        return v.toString().padStart(2, '0')
    }
    return `${pad(x.getFullYear())}-${pad(x.getMonth()+1)}-${pad(x.getDate())} ${pad(x.getHours())}:${pad(x.getMinutes())}:${pad(x.getSeconds())}`
}

async function gen_txhist_data_2(db_name, start, count, count_per_block, num_accounts, device) {
    var rdb = await rocksdb_open(db_name, {
            createIfMissing: true, 
            cacheSize: 1024 * 1024 * 1024,
            maxFileSize: 64 * 1024 * 1024
    })

    var types = [ "POOL", "SWAP", "GRANDStaking", "DIOSStaking", "ZAPIN", "ZAPOUT", "DIOS", "STWEMIX", "SETSTWEMIX", "SETStaking", "SETLENDING", "LENDING" ]
    var tokens = []
    for (var i = 0; i < 20; i++) {
        tokens.push((await sha256("" + i)).slice(0, 42))
    }

    var query = async function(stmt) {
        try {
            var v = await conn.promise().query(stmt)
            // TODO: check errors
            // console.log(v)
        } catch (e) {
            console.log(`failed: ${e}`)
        }
    }

    var start_time = (new Date()).getTime() - count * 1000
    var id = start, n_written = 0, per_written = 10000, last_written = 0
    var t = process.hrtime(), stats = getDiskIOStatsForDevice(device)
    var out = async function(title, cnt) {
        const ct = Math.floor((new Date()).getTime() / 1000)
        const dt = process.hrtime(t), cstats = getDiskIOStatsForDevice(device)
        const dtms = dt[0] * 1000 + dt[1] / 1000000
        const rps = Math.floor(cnt / (dtms / 1000))
        const readsPerSec = Math.floor((cstats.reads - stats.reads) / (dtms / 1000));
        const writesPerSec = Math.floor((cstats.writes - stats.writes) / (dtms / 1000));
        const readBytesPerSec = Math.floor((cstats.readBytes - stats.readBytes) / (dtms / 1000));
        const writeBytesPerSec = Math.floor((cstats.writeBytes - stats.writeBytes) / (dtms / 1000));

        console.log(`${title},${ct},${id-cnt},${cnt},${rps},${readsPerSec},${readBytesPerSec},${writesPerSec},${writeBytesPerSec}`)

        t = process.hrtime()
        stats = cstats
    }
    console.log('operation,time,index,count,rps,reads,readBytes,writes,writeBytes')
    var batch = rdb.batch()
    for (var i = start; i < (start + count); i++) {
        for (var j = 0; j < count_per_block; j++) {
            var num = i
            var block_timestamp = toDateString(start_time + i * 1000)
            var ix = i * count_per_block + j
            var typ = types[ix % types.length]
            var token0 = tokens[ix % tokens.length];
            var token1 = tokens[(ix+1) % tokens.length];
            var tx_hash = await sha256("" + ix.toString())
            var address = (await sha256((id % num_accounts).toString())).slice(0, 42)
            var status = 1
            var func_sig = "abc_def"
            var input = {"abcdef": 123}
            input = JSON.stringify(input).replaceAll(',', '\\,')
            var price0 = "100000000000001111111"
            var price1 = "100000000000001111112"
            var value = "8888888888888888888888888"
            var data = {"type": typ, "abc" :"def", "xx": 111}
            data = JSON.stringify(data).replaceAll("\'", "\\\'")
            var now = toDateString(new Date())

            var date = (new Date()).getTime()
            var obj = [ id, num, block_timestamp, address, typ, tx_hash,
                        status, func_sig, input, address, token0, token1,
                        price0, price1, value, data, date, date ]
            var objData = JSON.stringify(data).replaceAll("\'", "\\\'")
            batch.put(`txhist.o.${id}`, objData)
            id++
            n_written++
        }

        if (n_written - last_written >= per_written) {
            await rdb.batchWrite(batch)
            out("insert", n_written - last_written)
            last_written = n_written
            batch = rdb.batch()
        }
    }
    if (n_written - last_written > 0) {
        await rdb.batchWrite(batch)
        out("insert", n_written - last_written)
        last_written = n_written
        batch = rdb.batch()
    }

    await rdb.close((err) => {
        if (err) {
            console.log("close", err)
        }
    })
}

function getOpts() {
    var opts = {}
    process.argv.forEach((val, index) => {
        if (val === '-i' && process.argv[index + 1]) {
            opts.interval = parseInt(process.argv[index + 1]) * 1000;
        }
        if (val === '-d' && process.argv[index + 1]) {
            opts.device = process.argv[index + 1];
        }
    })
    return opts
}

async function main() {
    // await rrr()
    // await load_data()
    // await query()
    // var db = await opendb()
    // await query(db, 'SWAP')
    // db.close((err) => {})

    function usage() {
        console.log(`Usage: node rdb.js [
	txhist_data_2 <db_name> <start> <count> <count-per-block> <num-accounts> <device>
	`)
    }

    var args = process.argv
    switch (args[2]) {
    case "txhist_data_2":
	// node rdb.js txhist_data_2 <db_name> <start> <count> <count-per-block> <num-accounts> <device>
        if (args.length < 9) {
            usage()
            return
        }
        var dbName = args[3]
        var start = parseInt(args[4])
        var count = parseInt(args[5])
        var countPerBlock = parseInt(args[6])
        var numAccounts = parseInt(args[7])
        var dev = args[8]
        await gen_txhist_data_2(dbName, start, count, countPerBlock, numAccounts, dev)
        break
    default:
        usage()
        break
    }
}

main()

// EOF
