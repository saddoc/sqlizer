#!/bin/bash

':' //; export NODE_OPTIONS=--experimental-repl-await;
':' //; export NODE_PATH=$(npm root -gq):$(npm root -q):.
':' //; [ $# -le 0 ] && exec "$(which node)" -r $0 || exec "$(which node)" -r $0 \$0 $*

// rdb.js
//

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

    db.open(opts, (err) => {
        if (err != null) {
            reject(err)
        } else {
            resolve(db)
        }
    })
    return o
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

opendb = async function() {
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

async function main() {
    // await rrr()
    // await load_data()
    // await query()
    // var db = await opendb()
    // await query(db, 'SWAP')
    // db.close((err) => {})
}

main()

// EOF
