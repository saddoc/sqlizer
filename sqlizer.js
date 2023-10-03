// sqlizer.js
// 
// 1. install node & npm
// curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - && sudo apt-get install -y nodejs
// npm install mysql2
//
// 2. mysql setup
// sudo apt-get install -y mysql-client mysql-server
// sudo chmod g+rx /var/run/mysqld
// sudo usermod -a -G mysql nxt
// sudo mysql -u root
//   CREATE USER 'admin'@'localhost' IDENTIFIED BY '';
//   GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
//   FLUSH PRIVILEGES; 
// 2.1 mysql conf
//   - move /var/lib/mysql to somewhere else
//   - /etc/mysql/myqsl.conf.d/mysqld.conf
//     pid-file        = /var/run/mysqld/mysqld.pid 
//     datadir	= /var/lib/mysql
//     local_infile = 1
//     -- myisam only?
//     key_buffer_size		= 16M
//     max_connections        = 151
//     table_open_cache       = 4000
//     query_cache_size
//     have_query_cache        NO
//     -- multiple of innodb_buffer_pool_instances * innodb_buffer_pool_chunk_size
//     innodb_buffer_pool_size 16G
//     
//   - /etc/mysql/myqsl.conf.d/mysql.conf
//     local_infile = 1
//
// 3. mysql profiling
// set profiling=1;
// show profiles;
// 
// 4. load data
// load data local infile '/var/lib/mysql-files/lending_events.data'
//   replace into table Lending_Events fields terminated by ',';
// 
// 5. DBs
// db   - normal
// dbig - huge 
//
// 6. data generation
// node sqlizer.js data > data.100k
// grep Lending_Events data.100k > lending.100k
// grep Lending_CWemix_Events data.100k > lending_cwemix.100k
// grep Lending_CstWemix_Events data.100k > lending_cstwemix.100k
// grep Lending_CWemixDollar_Events data.100k > lending_cwemixdollar.100k
// sudo mv lending_*.100k /var/lib/mysql-files
// load data infile '/var/lib/mysql-files/lending.100k'
//   replace into table db.Lending_Events fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending.100k'
//   replace into table db.Lending_Events_Latest fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending_cwemix.100k'
//   replace into table db.Lending_CWemix_Events fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending_cstwemix.100k'
//   replace into table db.Lending_CstWemix_Events fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending_cwemixdollar.100k'
//   replace into table db.Lending_CWemixDollar_Events fields terminated by ',';
//
// node sqlizer.js data > data.1m
// grep Lending_Events data.1m > lending.1m
// grep Lending_CWemix_Events data.1m > lending_cwemix.1m
// grep Lending_CstWemix_Events data.1m > lending_cstwemix.1m
// grep Lending_CWemixDollar_Events data.1m > lending_cwemixdollar.1m
// sudo mv lending_*.1m /var/lib/mysql-files
// load data infile '/var/lib/mysql-files/lending.1m'
//   replace into table dbig.Lending_Events fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending.1m'
//   replace into table dbig.Lending_Events_Latest fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending_cwemix.1m'
//   replace into table dbig.Lending_CWemix_Events fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending_cstwemix.1m'
//   replace into table dbig.Lending_CstWemix_Events fields terminated by ',';
// load data infile '/var/lib/mysql-files/lending_cwemixdollar.1m'
//   replace into table dbig.Lending_CWemixDollar_Events fields terminated by ',';
/*
grep Lending_Events data.1m > lending.1m
grep Lending_CWemix_Events data.1m > lending_cwemix.1m
grep Lending_CstWemix_Events data.1m > lending_cstwemix.1m
grep Lending_CWemixDollar_Events data.1m > lending_cwemixdollar.1m
sudo mv lending*.1m /var/lib/mysql-files/

load data infile '/var/lib/mysql-files/lending.100k'
  replace into table dbig.Lending_Events fields terminated by ',';
load data infile '/var/lib/mysql-files/lending.100k'
  replace into table dbig.Lending_Events_Latest fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_cwemix.100k'
  replace into table dbig.Lending_CWemix_Events fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_cstwemix.100k'
  replace into table dbig.Lending_CstWemix_Events fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_cwemixdollar.100k'
  replace into table dbig.Lending_CWemixDollar_Events fields terminated by ',';

create index ix1_CWemix       on db.Lending_CWemix_Events (event_name, borrower);
create index ix1_CstWemix     on db.Lending_CstWemix_Events (event_name, borrower);
create index ix1_CWemixDollar on db.Lending_CWemixDollar_Events
  (event_name, borrower);
create index ix1_Lending      on db.Lending_Events (type, event_name, borrower);

create index ix1_CWemix       on dbig.Lending_CWemix_Events (event_name, borrower);
create index ix1_CstWemix     on dbig.Lending_CstWemix_Events (event_name, borrower);
create index ix1_CWemixDollar on dbig.Lending_CWemixDollar_Events
  (event_name, borrower);
create index ix1_Lending      on dbig.Lending_Events (type, event_name, borrower);
*/


const crypto = require('crypto')
const fs = require('fs');
const http = require('node:http')
const https = require('node:https')
const mysql = require('mysql2')

var mysql_conn_params = {
    host: 'localhost',
    multipleStatements: true,
    user: 'admin',
    password: '',
    ssl: { rejectUnauthorized: false }
}

async function sha256(x) {
    return '0x' + crypto.createHash('sha256').update(x).digest('hex')
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

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function mquery(num_threads, count, query, verbose) {
    var params = { ...mysql_conn_params }
    params.connectionLimit = num_threads
    params.waitForConnections = true
    var pool = mysql.createPool(params)

    var resolveDone, doneCount = 0, executionTime = 0.0
    var done = new Promise(resolve => { resolveDone = resolve })
    var st = process.hrtime()
    for (var i = 0; i < count; i++) {
        /*
        (function(ix) {
            var ist = process.hrtime()
            pool.query(query, function(err, results, fields) {
                var idt = process.hrtime(ist)
                var dtms = idt[0] * 1000 + idt[1] / 1000000
                executionTime += dtms
                if (err)
                    throw err
                if (verbose)
                    console.log(`${ix}: ${Math.floor(dtms)}ms ${JSON.stringify(results)}`)
                if ((++doneCount) == count)
                    resolveDone()
            })
        })(i)
        */

        (function(ix) { 
            pool.getConnection(function(err, connn) {
                if (err)
                    throw err
                var ist = process.hrtime()
                connn.query(query, function(err, results, fields) {
                    pool.releaseConnection(connn)

                    var idt = process.hrtime(ist)
                    var dtms = idt[0] * 1000 + idt[1] / 1000000
                    executionTime += dtms
                    if (err)
                        throw err
                    if (verbose) {
                        var r
                        if (verbose == 1)
                            r = results.length <= 1 ? JSON.stringify(results[0]) : `count=${results.length}`
                        else
                            r = JSON.stringify(results, null, " ")
                        console.log(`${ix}: ${Math.floor(dtms)}ms ${r}`)
                    }
                    if ((++doneCount) == count)
                        resolveDone()
                })
            })
        })(i)
    }

    await done
    pool.end()

    var dt = process.hrtime(st)
    var mt = dt[0] * 1000 + dt[1] / 1000000
    if (mt == 0)
        mt = 1
    var pt = executionTime / count
    console.log(`${count} / ${Math.floor(mt)}ms = ${Math.floor(mt / count)}ms per query = ${Math.floor(count * 1000 / mt)} tps, average = ${Math.floor(pt)}ms`)
}

async function http_get(url, agent, callback) {
    return http.get(url, agent, (res) => {
        if (res.statusCode == 301 || res.statusCode == 302) {
            console.log(`OOO: redirect ${res.statusCode} ${url} -> ${res.headers.location}`)
            return http_get(res.headers.location, agent, callback)
        }

        let data = ''
        res.on('data', (chunk) => {
            data += chunk
        })
        res.on('end', () => {
            callback(res, data)
        })
    })
}

// test http server
async function http_server(port) {
    const host = "localhost"
    var data1k = null, data100k = null, data200k = null, data1m = null
    data1k = "abcdefghijklmnop".repeat(1024 / 16)
    data100k = data1k.repeat(100)
    data200k = data1k.repeat(200)
    data1m = data1k.repeat(1024)
    const handler = function (req, res) {
        var data = null
        switch (req.url) {
        case "/f1k":
            data = data1k
            break
        case "/f100k":
            data = data100k
            break
        case "/f200k":
            data = data200k
            break
        case "/f1m":
            data = data1m
            break
        default:
            data = "hello ... olleh"
            break
        }
        res.writeHead(200);
        res.end(data)
    }
    const server = http.createServer(handler);
    server.listen(port, host, () => {
        console.log(`Server is running on http://${host}:${port}`)
    })
}

async function mhttp_get(num_threads, count, url, verbose) {
    http.globalAgent.keepAlive = true
    http.globalAgent.maxSockets = num_threads

    var resolveDone, doneCount = 0, executionTime = 0.0
    var done = new Promise(resolve => { resolveDone = resolve })
    var st = process.hrtime()
    for (var i = 0; i < count; i++) {
        (function(ix) {
            var ist = process.hrtime()
            var req = http_get(url, http.globalAgent, (res, data) => {
                if (res.statusCode != 200) {
                    var idt = process.hrtime(ist)
                    var dtms = idt[0] * 1000 + idt[1] / 1000000
                    executionTime += dtms
                    if ((++doneCount) == count)
                        resolveDone()
                    console.log("XXX: ", res.statusCode)
                    throw `Got ${res.statusCode}`
                }
                if (verbose) {
                    console.log(`${ix}: ${res.statusCode} ${data}`)
                }
                var idt = process.hrtime(ist)
                var dtms = idt[0] * 1000 + idt[1] / 1000000
                executionTime += dtms
                if ((++doneCount) == count)
                    resolveDone()
            })
            /*
                .on('error', (err) => {
                throw err
            })
            */
        })(i)
    }

    await done

    var dt = process.hrtime(st)
    var mt = dt[0] * 1000 + dt[1] / 1000000
    if (mt == 0)
        mt = 1
    var pt = executionTime / count
    console.log(`${count} / ${Math.floor(mt)}ms = ${Math.floor(mt / count)}ms per request = ${Math.floor(count * 1000 / mt)} tps, average = ${Math.floor(pt)}ms`)
}

var drop_lending_tables_sql = `
DROP TABLE IF EXISTS Lending_CWemix_Events;
DROP TABLE IF EXISTS Lending_CWemixDollar_Events;
DROP TABLE IF EXISTS Lending_CstWemix_Events;
DROP TABLE IF EXISTS Lending_Events;
DROP TABLE IF EXISTS Lending_Events_Latest;
`

var create_lending_tables_sql = `
CREATE TABLE Lending_CWemix_Events (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  tx_logIdx VARCHAR(128) NOT NULL UNIQUE,
  tx_hash VARCHAR(128) NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp DATETIME NULL,
  event_name VARCHAR(24) NOT NULL,
  event_data JSON NOT NULL,
  borrower VARCHAR(128),
  accountBorrows INTEGER,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=innodb;
CREATE INDEX ix1_Lending_CWemix_Events ON Lending_CWemix_Events (event_name, borrower);

CREATE TABLE Lending_CstWemix_Events (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  tx_logIdx VARCHAR(128) NOT NULL UNIQUE,
  tx_hash VARCHAR(128) NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp DATETIME NULL,
  event_name VARCHAR(24) NOT NULL,
  event_data JSON NOT NULL,
  borrower VARCHAR(64),
  accountBorrows INTEGER,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=innodb;
CREATE INDEX ix1_Lending_CstWemix_Events ON Lending_CstWemix_Events (event_name, borrower);

CREATE TABLE Lending_CWemixDollar_Events (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  tx_logIdx VARCHAR(128) NOT NULL UNIQUE,
  tx_hash VARCHAR(128) NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp DATETIME NULL,
  event_name VARCHAR(24) NOT NULL,
  event_data JSON NOT NULL,
  borrower VARCHAR(64),
  accountBorrows INTEGER,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=innodb;
CREATE INDEX ix1_Lending_CWemixDollar_Events ON Lending_CWemixDollar_Events (event_name, borrower);

CREATE TABLE Lending_Events (
  type VARCHAR(24) NOT NULL,
  tx_logIdx VARCHAR(128) NOT NULL UNIQUE,
  tx_hash VARCHAR(128) NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp DATETIME NULL,
  event_name VARCHAR(24) NOT NULL,
  event_data JSON NOT NULL,
  borrower VARCHAR(64),
  accountBorrows INTEGER,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (tx_hash)
) ENGINE=innodb;
CREATE INDEX ix1_Lending_Events ON Lending_Events (type, event_name, borrower);

CREATE TABLE Lending_Events_Latest (
  type VARCHAR(24) NOT NULL,
  tx_logIdx VARCHAR(128) NOT NULL UNIQUE,
  tx_hash VARCHAR(128) NOT NULL,
  block_number INTEGER NOT NULL,
  block_timestamp DATETIME NULL,
  event_name VARCHAR(24) NOT NULL,
  event_data JSON NOT NULL,
  borrower VARCHAR(64),
  accountBorrows INTEGER,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (type, event_name, borrower)
) ENGINE=innodb;
CREATE INDEX ix1_Lending_Events_Latest ON Lending_Events_Latest (type, event_name, borrower);
`

// will create temporary files in the current directory
async function gen_lending_events(db_name, start, count, count_per_block, num_accounts) {
    const fs = require('fs')
    const process = require('process')
    const { spawnSync } = require("node:child_process")
    const fn_cwemix = `data.cwemix`
    const fn_cstwemix = `data.cstwemix`
    const fn_cwemixdollar = `data.cwemixdollar`
    const fn_lending = `data.lending`

    const cleanup = function() {
        try { fs.unlinkSync(fn_cwemix) } catch {}
        try { fs.unlinkSync(fn_cstwemix) } catch {}
        try { fs.unlinkSync(fn_cwemixdollar) } catch {}
        try { fs.unlinkSync(fn_lending) } catch {}
    }
    process.on('SIGINT', cleanup)
    
    const f_cwemix = fs.openSync(fn_cwemix, 'w', 0o600)
    const f_cstwemix = fs.openSync(fn_cstwemix, 'w', 0o600)
    const f_cwemixdollar = fs.openSync(fn_cwemixdollar, 'w', 0o600)
    const f_lending = fs.openSync(fn_lending, 'w', 0o600)

    var types = ["CWemix", "CstWemix", "CWemixDollar"]
    var events = ["Borrow", "RepayBorrow", "Lend"]
    var id = 1
    for (var i = start; i < start+count; i++) {
        var block_number = i
        var block_timestamp = toDateString(1677600000000 + i * 1000)

        for (var j of events) {
            for (var k = 0; k < count_per_block; k++) {
                for (var t of types) {
                    var tx_hash = await sha256(t + j + i.toString() + k.toString())
                    var borrower = (await sha256((id % num_accounts).toString())).slice(0, 42)
                    var accountBorrows = 1
                    var event_name = j
                    var event_data = {
                        "event": j,
                        "borrower": borrower,
                        "accountBorrows": accountBorrows
                    }
                    event_data = JSON.stringify(event_data)

                    var now = toDateString(new Date())
                    var escaped_event_data = event_data.replaceAll(',', '\\,')

                    var tx_logIdx = `Lending_${t}_Events ${t} ${j} ${i} ${k}`
                    var data = `${id},${tx_logIdx},${tx_hash},${block_number},${block_timestamp},${event_name},${escaped_event_data},${borrower},${accountBorrows},${now},${now}`
                    switch (t) {
                    case "CWemix":
                        fs.writeSync(f_cwemix, data + "\n")
                        break
                    case "CstWemix":
                        fs.writeSync(f_cstwemix, data + "\n")
                        break
                    case "CWemixDollar":
                        fs.writeSync(f_cwemixdollar, data + "\n")
                        break
                    }

                    tx_logIdx  = `Lending_Events ${t} ${j} ${i} ${k}`
                    data = `${t},${tx_logIdx},${tx_hash},${block_number},${block_timestamp},${event_name},${escaped_event_data},${borrower},${accountBorrows},${now},${now}`
                    fs.writeSync(f_lending, data + "\n")
                    id++
                }
            }
        }
    }
    fs.closeSync(f_cwemix)
    fs.closeSync(f_cstwemix)
    fs.closeSync(f_cwemixdollar)
    fs.closeSync(f_lending)

    // load the data
    const stmt = 
`load data local infile '${fn_cwemix}' replace into table
  ${db_name}.Lending_CWemix_Events fields terminated by ',';
select count(*) from ${db_name}.Lending_CWemix_Events;
load data local infile '${fn_cstwemix}' replace into table
  ${db_name}.Lending_CstWemix_Events fields terminated by ',';
select count(*) from ${db_name}.Lending_CstWemix_Events;
load data local infile '${fn_cwemixdollar}' replace into table
  ${db_name}.Lending_CWemixDollar_Events fields terminated by ',';
select count(*) from ${db_name}.Lending_CWemixDollar_Events;
load data local infile '${fn_lending}' replace into table
  ${db_name}.Lending_Events fields terminated by ',';
select count(*) from ${db_name}.Lending_Events;
load data local infile '${fn_lending}' replace into table
  ${db_name}.Lending_Events_Latest fields terminated by ',';
select count(*) from ${db_name}.Lending_Events_Latest;`

    try {
        console.log(stmt)
        var v = spawnSync("mysql", ["-u", "admin", "-e", stmt], { stdio: 'inherit' })
        console.log("all good")
    } catch (e) {
        console.log(`failed: ${e}`)
    } finally {
        process.removeListener('SIGINT', cleanup)
        cleanup()
    }

/** fails with connect timeout
    var conn = mysql.createConnection(mysql_conn_params)
    conn.connect()

    var loadData = async (db_name, table_name, file_name) => {
        var stmt = `load data local infile '${file_name}' replace into table
  ${db_name}.${table_name} fields terminated by ',';`
        console.log(stmt)
        var v = await conn.promise().query({
            sql: stmt,
            values: [],
            timeout: 3600 * 1000,
            infileStreamFactory: () => fs.createReadStream(file_name)
        })
    }

    try {
        console.log("cwemix")
        await loadData(db_name, "Lending_CWemix_Events", fn_cwemix)
        console.log("cstwemix")
        await loadData(db_name, "Lending_CstWemix_Events", fn_cstwemix)
        console.log("cwemixdollar")
        await loadData(db_name, "Lending_CWemixDollar_Events", fn_cwemixdollar)
        console.log("lending")
        await loadData(db_name, "Lending_Events", fn_lending)
        console.log("lending_latest")
        await loadData(db_name, "Lending_Events_Latest", fn_lending)
        console.log("all good")
    } catch (e) {
        console.log(`failed: ${e}`)
    } finally {
        conn.end()
        process.removeListener('SIGINT', cleanup)
        cleanup()
    }
*/
}

async function create_lending_tables(db_name, drop_if_exists) {
    var conn = mysql.createConnection(mysql_conn_params)
    conn.connect()
    var stmt = `USE ${db_name};`
    if (drop_if_exists)
        stmt += drop_lending_tables_sql
    stmt += create_lending_tables_sql
    /*
    conn.query("SHOW TABLES; SHOW DATABASES;", function(error, results, fields) {
        if (results) {
            var r = JSON.stringify(results, null, "  ")
            var f = JSON.stringify(fields, null, "  ")
            console.log(`YYY: results=${r}`)
        }
        if (error) {
            console.log(`XXX: error ${error}`)
        }
    })
    */
    try {
        var v = await conn.promise().query(stmt)
        console.log("all good")
    } catch (e) {
        console.log(`failed: ${e}`)
    } finally {
        conn.end()
    }
}

// db.Lending_Events, etc. 'type' and 'event' => 10,000 entries
// dbig.Lending_Events, etc. 'type' and 'event' => 100,000 entries
async function stress_lending_events() {
    var verbose = 0
    var args = [
        { db: "db", num_threads: 1, count: 100, ixs: [1, 999, 9999] },
        { db: "db", num_threads: 20, count: 10000, ixs: [1, 999] },
        { db: "db", num_threads: 20, count: 1000, ixs: [9999] },
        { db: "dbig", num_threads: 1, count: 100, ixs: [1, 999, 99999] },
        { db: "dbig", num_threads: 20, count: 10000, ixs: [1, 999] },
        { db: "dbig", num_threads: 20, count: 1000, ixs: [99999] }
    ]

    for (var x of args) {
        for (var ix of x.ixs) {
            console.log(`### ${x.num_threads} connections, ${x.count} times, Limit ${ix},1`)
            var stmt = `SELECT borrower, max(block_number) as num
    FROM ${x.db}.Lending_CWemix_Events WHERE event_name = 'Borrow'
    GROUP BY borrower LIMIT ${ix},1`
            console.log(`  @${stmt}`)
            await mquery(x.num_threads, x.count, stmt, verbose)
            stmt = `SELECT borrower, max(block_number) as num
    FROM ${x.db}.Lending_Events WHERE type = 'CWemix' AND event_name = 'Borrow'
    GROUP BY borrower LIMIT ${ix},1`
            console.log(`  @${stmt}`)
            await mquery(x.num_threads, x.count, stmt, verbose)
            stmt = `SELECT borrower, block_number as num
    FROM ${x.db}.Lending_Events_Latest
      WHERE type = 'CWemix' AND event_name = 'Borrow' LIMIT ${ix},1`
            console.log(`  @${stmt}`)
            await mquery(x.num_threads, x.count, stmt, verbose)
        }
    }
}

// tx history
const drop_txhist_tables_sql = `
DROP TABLE IF EXISTS LiquidationTxHistory;
DROP TABLE IF EXISTS TxHistory;
`

const create_txhist_tables_sql = `
CREATE TABLE LiquidationTxHistory (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_timestamp DATETIME NULL,
    address VARCHAR(44) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    user VARCHAR(44) NOT NULL,
    token0 VARCHAR(44) NOT NULL,
    token1 VARCHAR(44) NOT NULL,
    price0 DECIMAL(65) NOT NULL,
    price1 DECIMAL(65) NOT NULL,
    value DECIMAL(65) NOT NULL,
    liquidation_data JSON NOT NULL,
    event_data JSON NOT NULL,
    create_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE TxHistory (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_timestamp DATETIME NULL,
    address VARCHAR(44) NOT NULL,
    type VARCHAR(44) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    status BOOL NOT NULL,
    func_sig VARCHAR(32) NOT NULL,
    input JSON NOT NULL,
    user VARCHAR(44) NOT NULL,
    token0 VARCHAR(44) NOT NULL,
    token1 VARCHAR(44) NOT NULL,
    price0 DECIMAL(65) NOT NULL,
    price1 DECIMAL(65) NOT NULL,
    value DECIMAL(65) NOT NULL,
    data JSON NOT NULL,
    create_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- PRIMARY KEY (id),
    UNIQUE KEY tx_hash (tx_hash),
    KEY block_number (block_number,address)
);

CREATE TABLE liquidationtxhistory (
  id int NOT NULL AUTO_INCREMENT,
  block_number int NOT NULL,
  block_timestamp datetime DEFAULT NULL,
  address varchar(255) NOT NULL,
  tx_hash varchar(255) NOT NULL,
  user varchar(255) NOT NULL,
  token0 varchar(255) NOT NULL,
  token1 varchar(255) NOT NULL,
  price0 decimal(65,0) NOT NULL,
  price1 decimal(65,0) NOT NULL,
  value decimal(65,0) DEFAULT NULL,
  liquidation_data json DEFAULT NULL,
  event_data json DEFAULT NULL,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);

CREATE TABLE txhistory (
  id int NOT NULL AUTO_INCREMENT,
  block_number int NOT NULL,
  block_timestamp datetime DEFAULT NULL,
  address varchar(44) NOT NULL,
  type varchar(32) NOT NULL,
  tx_hash varchar(66) NOT NULL,
  status tinyint(1) NOT NULL,
  func_sig varchar(16) NOT NULL,
  input json NOT NULL,
  user varchar(44) NOT NULL,
  token0 varchar(44) NOT NULL,
  token1 varchar(44) NOT NULL,
  price0 decimal(65,0) NOT NULL,
  price1 decimal(65,0) NOT NULL,
  value decimal(65,0) DEFAULT NULL,
  data json DEFAULT NULL,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY tx_hash (tx_hash),
  KEY block_number (block_number,address),
  INDEX (type, token0, token1, user, id DESC),
  INDEX (user, type, token0, token1, id DESC)
);`

const xxx_tx_counts = `
-- maybe need sum
-- " " indicates all for type, token and user
-- <type>, <token>, <user>
--   " ", " ", " "       -> all
--   "type", " ", " "    -> all for the type
--   " ", " ", "user"    -> all for the user
--   "type", " ", "user" -> all for the user
-- time_unit := (0, 1, 60, 3600, 86400)
-- time_slot := (0, floored block_time)
CREATE TABLE TxCounts (
      type VARCHAR(16),
      token VARCHAR(44),
      user VARCHAR(44),
      amount INT,
      count INT,
      block_number INT,
      block_time TIMESTAMP,
      time_unit INT,
      time_slot INT,
      updated_time TIMESTAMP,

      PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token ASC, user ASC),
      INDEX (time_unit ASC, type ASC, token ASC, user ASC, time_slot DESC),
      INDEX (block_number DESC)
);`

async function create_txhist_tables(db_name, drop_if_exists) {
    var conn = mysql.createConnection(mysql_conn_params)
    conn.connect()
    var stmt = `USE ${db_name};`
    if (drop_if_exists)
        stmt += drop_txhist_tables_sql
    stmt += create_txhist_tables_sql
    try {
        var v = await conn.promise().query(stmt)
        console.log("all good")
    } catch (e) {
        console.log(`failed: ${e}`)
    } finally {
        conn.end()
    }
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

async function gen_txhist_data(db_name, start, count, count_per_block, num_accounts) {
    const fs = require('fs')
    const process = require('process')
    const { spawnSync } = require("node:child_process")
    const fn1 = `data.txhistory`, fn2 = `data.liquidationtxhistory`

    const cleanup = function() {
        try { fs.unlinkSync(fn1) } catch {}
        try { fs.unlinkSync(fn2) } catch {}
    }
    process.on('SIGINT', cleanup)

    const f1 = fs.openSync(fn1, 'w', 0o600)
    const f2 = fs.openSync(fn2, 'w', 0o600)

    var types = [ "POOL", "SWAP", "GRANDStaking", "DIOSStaking", "ZAPIN", "ZAPOUT", "DIOS", "STWEMIX", "SETSTWEMIX", "SETStaking", "SETLENDING", "LENDING" ]
    var tokens = []
    for (var i = 0; i < 20; i++) {
        tokens.push((await sha256("" + i)).slice(0, 42))
    }

    var id = 1;
    for (var i = start; i < (start + count); i++) {
        for (var j = 0; j < count_per_block; j++) {
            var num = i
            var block_timestamp = toDateString(1677600000000 + i * 1000)
            var ix = i * count_per_block + j
            var typ = types[ix % types.length]
            var token0 = tokens[ix % tokens.length];
            var token1 = tokens[(ix+1) % tokens.length];
            var tx_hash = await sha256("" + ix.toString())
            var address = (await sha256((id % num_accounts).toString())).slice(0, 42)
            var status = 1
            var func_sig = token0.slice(0, 12)
            var input = {"abcdef": 123}
            input = JSON.stringify(input).replaceAll(',', '\\,')
            var price0 = "100000000000001111111"
            var price1 = "100000000000001111112"
            var value = "8888888888888888888888888"
            var tx_data = {"type": typ, "abc" :"def", "xx": 111}
            tx_data = JSON.stringify(tx_data).replaceAll(',', '\\,')
            var now = toDateString(new Date())

            var data
            if (typ == 'LENDING') {
                data = `${id},${num},${block_timestamp},${address},${tx_hash},${address},${token0},${token1},${price0},${price1},${value},${tx_data},${tx_data},${now},${now}`
                fs.writeSync(f2, data + "\n")
            }

            data = `${id},${num},${block_timestamp},${address},${typ},${tx_hash},${status},${func_sig},${input},${address},${token0},${token1},${price0},${price1},${value},${tx_data},${now},${now}`
            fs.writeSync(f1, data + "\n")

            id++
        }
    }

    fs.closeSync(f1)
    fs.closeSync(f2)

    // load the data
    const stmt =
`load data local infile '${fn1}' replace into table
  ${db_name}.txhistory fields terminated by ',';
select count(*) from ${db_name}.txhistory;
load data local infile '${fn2}' replace into table
  ${db_name}.liquidationtxhistory fields terminated by ',';
select count(*) from ${db_name}.liquidationtxhistory;`

    try {
        console.log(stmt)
        var v = spawnSync("mysql", ["-u", "admin", "-e", stmt], { stdio: 'inherit' })
        console.log("all good")
    } catch (err) {
        console.log(`failed: ${err}`)
    } finally {
        process.removeListener('SIGINT', cleanup)
        cleanup()
    }
}

async function gen_txhist_data_2(db_name, start, count, count_per_block, num_accounts, device) {
    var conn = mysql.createConnection(mysql_conn_params)
    conn.connect()

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

    var trunc = async function(ts) {
        ts = Math.floor(ts / 1000)
        var stmt = `
DELETE FROM ${db_name}.tx_counts
    WHERE time_unit = 1 AND block_time < '${toDateString((ts - 3600) * 1000)}';
DELETE FROM ${db_name}.tx_counts
    WHERE time_unit = 60 AND block_time < '${toDateString((ts - 86400) * 1000)}';
DELETE FROM ${db_name}.tx_counts
    WHERE time_unit = 3600 AND block_time < '${toDateString((ts - 2678400) * 1000)}';`
        await query(stmt)
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
    await query("START TRANSACTION;")
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

            var stmt = `INSERT INTO ${db_name}.txhistory (id, block_number, block_timestamp, address, type, tx_hash, status, func_sig, input, user, token0, token1, price0, price1, value, data, created_at, updated_at) VALUES (${id}, ${num}, '${block_timestamp}', '${address}', '${typ}', '${tx_hash}', ${status}, '${func_sig}', '${input}', '${address}', '${token0}', '${token1}', '${price0}', '${price1}', '${value}', '${data}', NOW(), NOW());`
            await query(stmt)
            id++
            n_written++
        }

        if (n_written - last_written >= per_written) {
            await query("COMMIT;")
            out("insert", n_written - last_written)
            await query(`CALL ${db_name}.tx_counts_proc();`)
            out("counts", n_written - last_written)
            last_written = n_written
            await query("START TRANSACTION;")
        }
    }
    if (n_written - last_written > 0) {
        await query("COMMIT;")
        out("insert", n_written - last_written)
        await query(`CALL ${db_name}.tx_counts_proc();`)
        out("counts", n_written - last_written)
        last_written = n_written
        await query("START TRANSACTION;")
    }
    await query("COMMIT;")

    conn.end()
}

async function main() {
    function usage() {
        console.log(`Usage: node sqlizer.js [
	lending_create <db_name> [<drop>] | lending_data | lending_stress |
	txhist_create <db_name> [<drop>] | txhist_data <db_name> | txhist_data_2 <db_name> | txhist_stress |
	mquery <num-threads> <count> <query> <verbose> |
	http-server <port> |
	mget <num-threads> <count> <url> <verbose>]`)
    }

    // 0: none, 1: count, 2: details
    const verbose = function(args, ix) {
        if (ix >= args.length)
            return 0
        var v = args[ix].toLowerCase()
        if (v == "false" || v == "f")
            return 0
        v = parseInt(v)
        if (v == 0)
            return 0
        else if (v == 1)
            return 1
        else if (v > 1)
            return 2
    }

    var args = process.argv
    switch (args[2]) {
    case "mquery":
        // node sqlizer.js mquery <num-threads> <count> <query> <verbose>
        if (args.length < 5) {
            usage()
            return
        }
        var num_threads = parseInt(args[3])
        var count = parseInt(args[4])
        await mquery(num_threads, count, args[5], verbose(args, 6))
        break
    case "http-server":
        // node sqlizer.js http-server <port>
        var port = parseInt(args[3])
        http_server(port)
        break
    case "mget":
        // node sqlizer.js mget <num-threads> <count> <url> <verbose>
        if (args.length < 5) {
            usage()
            return
        }
        var num_threads = parseInt(args[3])
        var count = parseInt(args[4])
        await mhttp_get(num_threads, count, args[5], verbose(args, 5))
        break
    case "lending_create":
        if (args.length < 4) {
            usage()
            return
        }
        var db_name = args[3]
        var drop_tables = verbose(args, 4)
        await create_lending_tables(db_name, drop_tables >= 1)
        break
    case "lending_data":
        // await gen_lending_events('db', 1, 1000, 1, 1)
        await gen_lending_events('db', 1, 100000, 1, 30000)
        await gen_lending_events('dbig', 1, 1000000, 1, 300000)
        break
    case "lending_stress":
        await stress_lending_events()
        break
    case "txhist_create":
        if (args.length < 4) {
            usage()
            return
        }
        var db_name = args[3]
        var drop_tables = verbose(args, 4)
        await create_txhist_tables(db_name, drop_tables >= 1)
        break
    case "txhist_data":
        if (args.length < 4) {
            usage()
            return
        }
        var db_name = args[3]
        // await gen_txhist_data(1, 1, 1, 1)
        // await gen_txhist_data(db_name, 1, 1000000, 10, 100000)
        // await gen_txhist_data(1, 1000000, 1, 300000)
        await gen_txhist_data(db_name, 1, 100000000, 10, 100000)
        break
    case "txhist_data_2":
        if (args.length < 4) {
            usage()
            return
        }
        var db_name = args[3]
        // await gen_txhist_data_2(db_name, 1, 1, 1, 30000)
        // await gen_txhist_data_2(db_name, 1, 4000, 1, 30000)
        // await gen_txhist_data_2(db_name, 1, 30000, 2, 30000)
        await gen_txhist_data_2(db_name, 1, 20000000, 10, 100000, 'sda')
        break
    case "txhist_stress":
        // TODO
        await stress_txhist_events()
        break
    default:
        usage()
        break
    }
}

main()

// EOF
