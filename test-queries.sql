CREATE DATABASE IF NOT EXISTS xyz;
USE xyz;

CREATE TABLE IF NOT EXISTS tab1 (
    name VARCHAR(32) PRIMARY KEY,
    value VARCHAR(32),
    i1 INT,
    i2 INT
);

CREATE TABLE IF NOT EXISTS tab2 (
    t2_name VARCHAR(32) PRIMARY KEY,
    t2_value VARCHAR(32),
    t2_i1 INT,
    t2_i2 INT
);

CREATE TABLE IF NOT EXISTS tab3 (
    name VARCHAR(32) PRIMARY KEY,
    value VARCHAR(32),
    i1 INT,
    i2 INT
) ENGINE=MyISAM;

-- CREATE INDEX tab1_ix1 ON tab1 (name, i1)
CREATE INDEX tab1_ix1 ON tab1 (i1, name)

SELECT name FROM tab1 WHERE name = 'abc';
SELECT name FROM tab1 WHERE value = 'abc';
SELECT name FROM tab1 WHERE name = 'abc' AND i1 = 100;
SELECT name FROM tab1 WHERE name = 'abc' AND i1 = 100 AND i2 = 100;

SELECT tab1.name FROM tab1 LEFT JOIN tab2 ON (tab1.name = tab2.t2_name) WHERE tab1.name = 'abc';

# user privileges
chmod g+rx /var/run/mysqld
add nxt to mysql group
create user 'admin'@'localhost' IDENTIFIED BY '';
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
flush privileges; 

load data infile '/var/lib/mysql-files/tab3.data' into table tab3 fields terminated by ',';
load data infile '/var/lib/mysql-files/xxx.data' replace into table Lending_Events fields terminated by ',';

load data infile '/var/lib/mysql-files/lending_events.data'
  replace into table Lending_Events fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_events.data'
  replace into table Lending_Events_Latest fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_cwemix_events.data'
  replace into table Lending_CWemix_Events fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_cstwemix_events.data'
  replace into table Lending_CstWemix_Events fields terminated by ',';
load data infile '/var/lib/mysql-files/lending_cwemixdollar_events.data'
  replace into table Lending_CWemixDollar_Events fields terminated by ',';


alter table tab3 ENGINE=MyISAM;
alter table tab3 ENGINE=innodb;
set profiling=1;
show profiles;

CREATE TABLE TxHistory (
) ENGINE=MyISAM;

CREATE TABLE LiquidationTxHistory (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_timestamp DATETIME NULL,
    address TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    user TEXT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    price0 DECIMAL(65) NOT NULL,
    price1 DECIMAL(65) NOT NULL,
    value DECIMAL(65) NOT NULL,
    liquidation_data JSON NOT NULL,
    event_data JSON NOT NULL,
    create_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=MyISAM;

CREATE TABLE TxHistory (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_timestamp DATETIME NULL,
    address TEXT NOT NULL,
    type TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    status BOOL NOT NULL,
    func_sig TEXT NOT NULL,
    input JSON NOT NULL,
    user TEXT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    price0 DECIMAL(65) NOT NULL,
    price1 DECIMAL(65) NOT NULL,
    value DECIMAL(65) NOT NULL,
    data JSON NOT NULL,
    create_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=MyISAM;



JOIN 말고 UNION 사용하는 sql 입니다

SELECT
            COUNT(*) num
            FROM
            (SELECT 
            block_number,
            block_timestamp,
            address, 
            "LENDING" AS type, 
            tx_hash, 
            TRUE AS status, 
            0 AS func_sig, 
            0 AS input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            liquidation_data AS data
            FROM LiquidationTxHistory
            WHERE block_number >= 18484275
            UNION DISTINCT
            SELECT 
            block_number,
            block_timestamp,
            address, 
            type, 
            tx_hash, 
            status, 
            func_sig, 
            input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            data 
            FROM TxHistory
            WHERE block_number >= 18484275) AS NEW
            WHERE ((token0 IN ("0x5bdB668DE9C160746c40c0528961a4ffc6231a59","0x244c72AB61f11dD44Bfa4AaF11e2EFD89ca789fe","0xAe81b9fFCde5Ab7673dD4B2f5c648a5579430B17","0x53455405946657d7D77D22D552120e60d9aeC218","0x746663e9770e554b56Cc32a4A33680bFF0082Cff","0xA40b22C05Dd915BAeb83e4b633C59E37C6071d16","0x4aF286b2BF66AE46dA121fFeB46c2a3BbE05DD97","0x974D63275013fd863992e7245572E9aedB3319D5","0xE19B799146276Fd8ba7Bf807347A33Ef7Fd49B4b","0x9d88364cE61172D5398cD99c96b8D74899943fF4","0x4e202313790ae15AE84A7E5716EbFbB358C43530","0xE8C5201f0f4bdde271ACC9F14057CB4fe0DBA616","0x0000000000000000000000000000000000000000")) AND (token1 IN ("0x5bdB668DE9C160746c40c0528961a4ffc6231a59","0x244c72AB61f11dD44Bfa4AaF11e2EFD89ca789fe","0xAe81b9fFCde5Ab7673dD4B2f5c648a5579430B17","0x53455405946657d7D77D22D552120e60d9aeC218","0x746663e9770e554b56Cc32a4A33680bFF0082Cff","0xA40b22C05Dd915BAeb83e4b633C59E37C6071d16","0x4aF286b2BF66AE46dA121fFeB46c2a3BbE05DD97","0x974D63275013fd863992e7245572E9aedB3319D5","0xE19B799146276Fd8ba7Bf807347A33Ef7Fd49B4b","0x9d88364cE61172D5398cD99c96b8D74899943fF4","0x4e202313790ae15AE84A7E5716EbFbB358C43530","0xE8C5201f0f4bdde271ACC9F14057CB4fe0DBA616","0x0000000000000000000000000000000000000000")))
SELECT
SELECT
SELECT
            COUNT(*) num
            FROM
            (SELECT 
            block_number,
            block_timestamp,
            address, 
            "LENDING" AS type, 
            tx_hash, 
            TRUE AS status, 
            0 AS func_sig, 
            0 AS input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            liquidation_data AS data
            FROM LiquidationTxHistory
            WHERE block_number >= 18484275
            UNION DISTINCT
            SELECT 
            block_number,
            block_timestamp,
            address, 
            type, 
            tx_hash, 
            status, 
            func_sig, 
            input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            data 
            FROM TxHistory
            WHERE block_number >= 18484275) AS NEW
            COUNT(*) num
            FROM
            (SELECT 
            block_number,
            block_timestamp,
            address, 
            "LENDING" AS type, 
            tx_hash, 
            TRUE AS status, 
            0 AS func_sig, 
            0 AS input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            liquidation_data AS data
            FROM LiquidationTxHistory
            WHERE block_number >= 18484275
            UNION DISTINCT
            SELECT 
            block_number,
            block_timestamp,
            address, 
            type, 
            tx_hash, 
            status, 
            func_sig, 
            input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            data 
            FROM TxHistory
            WHERE block_number >= 18484275) AS NEW
            COUNT(*) num
            FROM
            (SELECT 
            block_number,
            block_timestamp,
            address, 
            "LENDING" AS type, 
            tx_hash, 
            TRUE AS status, 
            0 AS func_sig, 
            0 AS input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            liquidation_data AS data
            FROM LiquidationTxHistory
            WHERE block_number >= 18484275
            UNION DISTINCT
            SELECT 
            block_number,
            block_timestamp,
            address, 
            type, 
            tx_hash, 
            status, 
            func_sig, 
            input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            data 
            FROM TxHistory
            WHERE block_number >= 18484275) AS NEW

SELECT A.borrower, max(A.max_block) as max_block
  FROM (
    SELECT json_extract(event_data,'$.borrower') as borrower, max(block_number) as max_block
      FROM wemixfi.Lending_CWemix_Events where event_name like 'Borrow'
      group by borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as borrower, max(block_number) as max_block
      FROM wemixfi.Lending_CWemixDollar_Events where event_name like 'Borrow'
      group by borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as borrower, max(block_number) as max_block
      FROM wemixfi.Lending_CstWemix_Events where event_name like 'Borrow'
      group by borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as borrower, max(block_number) as max_block
      FROM wemixfi.Lending_CUSDC_Events
      where event_name like 'Borrow'
      group by borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as borrower, max(block_number) as max_block
      FROM wemixfi.Lending_CUSDT_Events where event_name like 'Borrow'
      group by borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as borrower, max(block_number) as max_block
      FROM wemixfi.Lending_CwRFT_Events
      where event_name like 'Borrow'
      group by borrower) AS A
  GROUP BY A.borrower ORDER BY A.borrower;


SELECT A.repayer as repayer, max(A.max_block) as max_block
  FROM(
    SELECT json_extract(event_data,'$.borrower') as repayer ,max(block_number) as max_block, json_extract(event_data,'$.accountBorrows') as account_borrows
      FROM wemixfi.Lending_CWemix_Events
      where event_name like 'RepayBorrow'
      group by repayer, account_borrows
      having account_borrows = '0'
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as repayer ,max(block_number) as max_block, json_extract(event_data,'$.accountBorrows') as account_borrows
      FROM wemixfi.Lending_CWemixDollar_Events
      where event_name like 'RepayBorrow'
      group by repayer, account_borrows
      having account_borrows = '0'
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as repayer ,max(block_number) as max_block, json_extract(event_data,'$.accountBorrows') as account_borrows
      FROM wemixfi.Lending_CstWemix_Events where event_name like 'RepayBorrow'
      group by repayer, account_borrows
      having account_borrows = '0'
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as repayer ,max(block_number) as max_block, json_extract(event_data,'$.accountBorrows') as account_borrows
      FROM wemixfi.Lending_CUSDC_Events where event_name like 'RepayBorrow'
      group by repayer, account_borrows
      having account_borrows = '0'
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as repayer ,max(block_number) as max_block, json_extract(event_data,'$.accountBorrows') as account_borrows
      FROM wemixfi.Lending_CUSDT_Events where event_name like 'RepayBorrow'
      group by repayer, account_borrows
      having account_borrows = '0'
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as repayer ,max(block_number) as max_block, json_extract(event_data,'$.accountBorrows') as account_borrows
      FROM wemixfi.Lending_CwRFT_Events
      where event_name like 'RepayBorrow'
      group by repayer, account_borrows
      having account_borrows = '0') AS A
  GROUP BY A.repayer ORDER BY A.repayer

SELECT 
 A.borrower, max(A.max_block) as max_block 
FROM(
 SELECT json_extract(event_data,'$.borrower') as borrower, 
  max(block_number) as max_block 
 FROM wemixfi.Lending_CWemix_Events 
 where event_name like 'Borrow' group by borrower  
 UNION ALL 
 SELECT json_extract(event_data,'$.borrower') as borrower, 
  max(block_number) as max_block 
 FROM wemixfi.Lending_CWemixDollar_Events 
 where event_name like 'Borrow' 
 group by borrower  
 UNION ALL 
 SELECT json_extract(event_data,'$.borrower') as borrower, 
  max(block_number) as max_block 
 FROM wemixfi.Lending_CstWemix_Events 
 where event_name like 'Borrow' 
 group by borrower  
 UNION ALL 
 SELECT json_extract(event_data,'$.borrower') as borrower, 
  max(block_number) as max_block 
 FROM wemixfi.Lending_CUSDC_Events 
 where event_name like 'Borrow' 
 group by borrower 
 UNION ALL 
 SELECT json_extract(event_data,'$.borrower') as borrower, 
  max(block_number) as max_block 
 FROM wemixfi.Lending_CUSDT_Events 
 where event_name like 'Borrow' 
 group by borrower  
 UNION all
 SELECT json_extract(event_data,'$.borrower') as borrower, 
  max(block_number) as max_block 
 FROM wemixfi.Lending_CwRFT_Events 
 where event_name like 'Borrow' 
 group by borrower  
) AS A 
GROUP BY A.borrower ORDER BY A.borrower;



SELECT A.x_borrower, max(A.max_block) as max_block
  FROM (
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemix_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemixDollar_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CstWemix_Events where event_name like 'Borrow'
      group by x_borrower) AS A
  GROUP BY A.x_borrower ORDER BY A.x_borrower;



SELECT json_extract(event_data,'$.borrower') as x_borrower,
       max(block_number) as max_block
  FROM xyz.Lending_Events where event_name like 'Borrow'
  group by x_borrower;
SELECT borrower as x_borrower, max(block_number) as max_block
  FROM xyz.Lending_Events where event_name = 'Borrow'
  group by x_borrower;
SELECT borrower as x_borrower, block_number
  FROM xyz.Lending_Events where event_name = 'Borrow';

SELECT borrower, block_number FROM Lending_Events
  WHERE type = 'CWemix' and event_name = 'Borrow';
SELECT json_extract(event_data, '$.borrower'), block_number FROM Lending_Events
  WHERE type = 'CWemix' and event_name = 'Borrow';

SELECT json_extract(event_data,'$.borrower') as x_borrower,
       max(block_number) as max_block
  FROM xyz.Lending_Events where event_name like 'Borrow'
  group by x_borrower;


- tables: Lending_CWemix_Events, Lending_CstWemix_Events, Lending_CWemixDollar_Events
  - combined tables: Lending_Events, Lending_Events_Latest
    - Lending_Events_Latest is to avoid group by, so as to reduce scan size
  - 

  



- count
  - SELECT COUNT(*) WHERE ... is no no
  - special "SELECT COUNT(*) FROM <table>" with MyISAM is very fast, but not flexible
  - need to maintain separate counters, e.g.
    START TRANSACTION
    INSERT INTO ...
    INSERT INTO ...
    UPDATE counter_table SET tx_counter = tx_counter + 1;
    COMMIT

mysql> show profiles;
+----------+------------+------------------------------------------+
| Query_ID | Duration   | Query                                    |
+----------+------------+------------------------------------------+
|        1 | 0.02083675 | select count(*) from Lending_Events      |
|        2 | 4.54968900 | alter table Lending_Events ENGINE=MyISAM |
|        3 | 0.00025475 | select count(*) from Lending_Events      |
+----------+------------+------------------------------------------+
3 rows in set, 1 warning (0.00 sec)


$ node ./gen-data.js create | sudo mysql -u root xyz       
$ echo "load data infile '/var/lib/mysql-files/lending_even ts.data'
  replace into table Lending_Events fields terminated by ',';                    
load data infile '/var/lib/mysql-files/lending_events.data'                      
  replace into table Lending_Events_Latest fields terminated by ',';             
load data infile '/var/lib/mysql-files/lending_cwemix_events.data'               
  replace into table Lending_CWemix_Events fields terminated by ',';             
load data infile '/var/lib/mysql-files/lending_cstwemix_events.data'             
  replace into table Lending_CstWemix_Events fields terminated by ',';           
load data infile '/var/lib/mysql-files/lending_cwemixdollar_events.data'         
  replace into table Lending_CWemixDollar_Events fields terminated by ',';       
" | sudo mysql -u root xyz    

drop index ix1_CWemix        on Lending_CWemix_Events;
drop index ix1_CstWemix      on Lending_CstWemix_Events;
drop index ix1_CWemixDollar  on Lending_CWemixDollar_Events;
drop index ix1_Lending       on Lending_Events;
drop index ix1_Latest        on Lending_Events_Latest;

create index ix1_CWemix       on Lending_CWemix_Events (event_name, borrower);
create index ix1_CstWemix     on Lending_CstWemix_Events (event_name, borrower);
create index ix1_CWemixDollar on Lending_CWemixDollar_Events
  (event_name, borrower);
create index ix1_Lending      on Lending_Events (type, event_name, borrower);
create index ix1_Latest    on Lending_Events_Latest (type, event_name, borrower);


|        5 | 0.01533825 | select borrower, block_number from Lending_CWemix_Event
s where event_name = 'Borrow'                           |
|        6 | 0.01226300 | select borrower, block_number from Lending_CWemix_Event
s where event_name = 'Borrow' limit 9999,1              |
|        7 | 0.04024050 | select borrower, block_number from Lending_Events where
 type = 'CWemix' and event_name = 'Borrow' limit 9999,1 |
+

|       43 | 0.02079775 | select borrower, max(block_number) as num from Lending_
CWemix_Events where event_name = 'Borrow' group by borrower limit 999,1
    |
|       44 | 0.03621025 | select borrower, max(block_number) as num from Lending_
Events where type = 'CWemix' and event_name = 'Borrow' group by borrower limit 99
9,1 |
|       46 | 0.00118075 | select borrower, block_number as num from Lending_Event
s_Latest where type = 'CWemix' and event_name = 'Borrow' limit 999,1



|       48 | 0.00252050 | select borrower, max(block_number) as num from Lending_CWemix_Events where event_name = 'Borrow' group by borrower limit 100,1
    |
|       49 | 0.00455150 | select borrower, max(block_number) as num from Lending_
    Events where type = 'CWemix' and event_name = 'Borrow' group by borrower limit 100,1 |
|       50 | 0.00061825 | select borrower, block_number as num from Lending_Events_Latest where type = 'CWemix' and event_name = 'Borrow' limit 100,1 |

|       52 | 0.00254875 | select borrower, max(block_number) as num from Lending_CWemix_Events where event_name = 'Borrow' group by borrower order by borrower limit 100,1              |
|       53 | 0.00438825 | select borrower, max(block_number) as num from Lending_Events where type = 'CWemix' and event_name = 'Borrow' group by borrower order by borrower limit 100,1 |
|       54 | 0.00061675 | select borrower, block_number as num from Lending_Events_Latest where type = 'CWemix' and event_name = 'Borrow' order by borrower limit 100,1                 |

|       58 | 0.02071525 | select borrower, max(block_number) as num from Lending_
CWemix_Events where event_name = 'Borrow' group by borrower order by num limit 10
0,1                   |
|       59 | 0.03108400 | select borrower, max(block_number) as num from Lending_
Events where type = 'CWemix' and event_name = 'Borrow' group by borrower order by
 num limit 100,1      |
|       60 | 0.00109450 | select borrower, block_number as num from Lending_Event
s_Latest where type = 'CWemix' and event_name = 'Borrow' order by num limit 100,1
                      |

mysql> explain select borrower, max(block_number) as num from Lending_Events wher
e type = 'CWemix' and event_name = 'Borrow' group by borrower order by num limit
100,1;
+----+-------------+----------------+------------+------+---------------+--------
-----+---------+-------------+-------+----------+--------------------------------
-+
| id | select_type | table          | partitions | type | possible_keys | key
     | key_len | ref         | rows  | filtered | Extra
     |
    +----+-------------+----------------+------------+------+---------------+--------
    -----+---------+-------------+-------+----------+--------------------------------
    -+
    |  1 | SIMPLE      | Lending_Events | NULL       | ref  | ix1_Lending   | ix1_Len
    ding | 196     | const,const | 20840 |   100.00 | Using temporary; Using filesort
     |
    +----+-------------+----------------+------------+------+---------------+--------
    -----+---------+-------------+-------+----------+--------------------------------
    -+
mysql> explain select borrower, max(block_number) as num from Lending_Events wher
e type = 'CWemix' and event_name = 'Borrow' group by borrower order by borrower l
imit 100,1;
+----+-------------+----------------+------------+------+---------------+--------
-----+---------+-------------+-------+----------+-------+
| id | select_type | table          | partitions | type | possible_keys | key
     | key_len | ref         | rows  | filtered | Extra |
    +----+-------------+----------------+------------+------+---------------+--------
    -----+---------+-------------+-------+----------+-------+
    |  1 | SIMPLE      | Lending_Events | NULL       | ref  | ix1_Lending   | ix1_Len
    ding | 196     | const,const | 20840 |   100.00 | NULL  |
    +----+-------------+----------------+------------+------+---------------+--------
    -----+---------+-------------+-------+----------+-------+


|       76 | 0.03695300 | SELECT json_extract(event_data, '$.borrower') as x_borr
ower, max(block_number) as num FROM Lending_Events   WHERE type = 'CWemix' and ev
ent_name = 'Borrow' group by x_borrower limit 999,1  |
|       77 | 0.03009850 | SELECT borrower as x_borrower, max(block_number) as num
 FROM Lending_Events   WHERE type = 'CWemix' and event_name = 'Borrow' group by x
_borrower limit 999,1                                |
|       78 | 0.03515700 | SELECT json_extract(event_data, '$.borrower') as x_borr
ower, max(block_number) as num FROM Lending_Events   WHERE type = 'CWemix' and ev
ent_name = 'Borrow' group by x_borrower limit 1,1    |
|       79 | 0.00075050 | SELECT borrower as x_borrower, max(block_number) as num
 FROM Lending_Events   WHERE type = 'CWemix' and event_name = 'Borrow' group by x
_borrower limit 1,1                                  |


-- without indexes
|       89 | 0.02010275 | select borrower, max(block_number) as num from Lending_
CWemix_Events where event_name = 'Borrow' group by borrower order by borrower lim
it 100,1              |
|       90 | 0.04932600 | select borrower, max(block_number) as num from Lending_
Events where type = 'CWemix' and event_name = 'Borrow' group by borrower order by
 borrower limit 100,1 |
|       91 | 0.04364675 | select borrower, block_number as num from Lending_Event
s where type = 'CWemix' and event_name = 'Borrow' order by borrower limit 100,1  
                      |
|       92 | 0.00058600 | select borrower, block_number as num from Lending_Event
s_Latest where type = 'CWemix' and event_name = 'Borrow' order by borrower limit 
100,1                 |


-- count(*): 9000, 3000, 1000 respectively
|      102 | 0.00185600 | select count(*) from Lending_Events_Latest             
                                                                                 
                      |
|      103 | 0.00184075 | select count(*) from Lending_Events_Latest where type =
 'CWemix'                                                                        
                      |
|      104 | 0.00101450 | select count(*) from Lending_Events_Latest where type =
 'CWemix' and event_name = 'Borrow'                                              
                      |


SELECT A.x_borrower, max(A.max_block) as max_block
  FROM (
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemix_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemixDollar_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CstWemix_Events where event_name like 'Borrow'
      group by x_borrower) AS A
  GROUP BY A.x_borrower ORDER BY A.x_borrower;

SELECT A.x_borrower, max(A.max_block) as max_block
  FROM (
    SELECT borrower as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemix_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT borrower as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemixDollar_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT borrower as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CstWemix_Events where event_name like 'Borrow'
      group by x_borrower) AS A
  GROUP BY A.x_borrower ORDER BY A.x_borrower;

|      112 | 0.09006575 | SELECT A.x_borrower, max(A.max_block) as max_block   FR
OM (     SELECT json_extract(event_data,'$.borrower') as x_borrower,            m
ax(block_number) as max_block       FROM xyz.Lending_CWemix_Events where event_na
me like 'Borrow'       group by x_borrower     UNION ALL     SELECT json_extract(
ev |
|      113 | 0.09065000 | SELECT A.x_borrower, max(A.max_block) as max_block   FR
OM (     SELECT json_extract(event_data,'$.borrower') as x_borrower,            m
ax(block_number) as max_block       FROM xyz.Lending_CWemix_Events where event_na
me like 'Borrow'       group by x_borrower     UNION ALL     SELECT json_extract(
ev |


|      121 | 0.08971550 | SELECT A.x_borrower, max(A.max_block) as max_block   FR
OM (     SELECT json_extract(event_data,'$.borrower') as x_borrower,            m
ax(block_number) as max_block       FROM xyz.Lending_CWemix_Events where event_na
me like 'Borrow'       group by x_borrower     UNION ALL     SELECT json_extract(
ev |
|      122 | 1.55183900 | create index ix1_CWemix       on Lending_CWemix_Events 
(event_name, borrower)                                                           
                                                                                 
                                                                                 
   |
|      123 | 1.53379025 | create index ix1_CstWemix     on Lending_CstWemix_Event
s (event_name, borrower)                                                         
                                                                                 
                                                                                 
   |
|      124 | 1.53331000 | create index ix1_CWemixDollar on Lending_CWemixDollar_E
vents
  (event_name, borrower)                                                         
                                                                                 
                                                                              |
|      125 | 0.08789875 | SELECT A.x_borrower, max(A.max_block) as max_block   FR
OM (     SELECT json_extract(event_data,'$.borrower') as x_borrower,            m
ax(block_number) as max_block       FROM xyz.Lending_CWemix_Events where event_na
me like 'Borrow'       group by x_borrower     UNION ALL     SELECT json_extract(
ev |



mysql> explain select borrower, max(block_number) as num
         from Lending_CWemix_Events where event_name = 'Borrow'
         group by borrower order by num limit 1,1;
|  1 | SIMPLE      | Lending_CWemix_Events | NULL       | ref  | ix1_CWemix    | 
ix1_CWemix | 98      | const | 14711 |   100.00 | Using temporary; Using filesort
mysql> explain select borrower, max(block_number) as num
         from Lending_CWemix_Events where event_name = 'Borrow'
         group by borrower limit 1,1;
|  1 | SIMPLE      | Lending_CWemix_Events | NULL       | ref  | ix1_CWemix    | 
ix1_CWemix | 98      | const | 14711 |   100.00 | NULL  |

|      167 | 0.02060000 | select borrower, max(block_number) as num from Lending_
CWemix_Events where event_name = 'Borrow' group by borrower order by num limit 1,
1              |
|      168 | 0.03100750 | select borrower, max(block_number) as num from Lending_
Events where type = 'CWemix' and event_name = 'Borrow' group by borrower order by
 num limit 1,1 |


|      171 | 0.08831950 | SELECT A.x_borrower, max(A.max_block) as max_block
  FROM (
    SELECT json_extract(event_data,'$.borrower') as x_borrower,
           max(block_number) as max_block
      FROM xyz.Lending_CWemix_Events where event_name like 'Borrow'
      group by x_borrower
    UNION ALL
    SELECT json_extract(ev |



|      175 | 0.08811525 | SELECT ...
|      176 | 0.05677050 | SELECT ...

SELECT
            COUNT(*) num
            FROM
            (SELECT 
            block_number,
            block_timestamp,
            address, 
            "LENDING" AS type, 
            tx_hash, 
            TRUE AS status, 
            0 AS func_sig, 
            0 AS input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            liquidation_data AS data
            FROM LiquidationTxHistory
            WHERE block_number >= 1
            UNION DISTINCT
            SELECT 
            block_number,
            block_timestamp,
            address, 
            type, 
            tx_hash, 
            status, 
            func_sig, 
            input, 
            user, 
            token0, 
            token1, 
            price0, 
            price1, 
            data 
            FROM TxHistory
            WHERE block_number >= 1) AS NEW

            WHERE ((token0 IN ("0x5bdB668DE9C160746c40c0528961a4ffc6231a59","0x244c72AB61f11dD44Bfa4AaF11e2EFD89ca789fe","0xAe81b9fFCde5Ab7673dD4B2f5c648a5579430B17","0x53455405946657d7D77D22D552120e60d9aeC218","0x746663e9770e554b56Cc32a4A33680bFF0082Cff","0xA40b22C05Dd915BAeb83e4b633C59E37C6071d16","0x4aF286b2BF66AE46dA121fFeB46c2a3BbE05DD97","0x974D63275013fd863992e7245572E9aedB3319D5","0xE19B799146276Fd8ba7Bf807347A33Ef7Fd49B4b","0x9d88364cE61172D5398cD99c96b8D74899943fF4","0x4e202313790ae15AE84A7E5716EbFbB358C43530","0xE8C5201f0f4bdde271ACC9F14057CB4fe0DBA616","0x0000000000000000000000000000000000000000")) AND (token1 IN ("0x5bdB668DE9C160746c40c0528961a4ffc6231a59","0x244c72AB61f11dD44Bfa4AaF11e2EFD89ca789fe","0xAe81b9fFCde5Ab7673dD4B2f5c648a5579430B17","0x53455405946657d7D77D22D552120e60d9aeC218","0x746663e9770e554b56Cc32a4A33680bFF0082Cff","0xA40b22C05Dd915BAeb83e4b633C59E37C6071d16","0x4aF286b2BF66AE46dA121fFeB46c2a3BbE05DD97","0x974D63275013fd863992e7245572E9aedB3319D5","0xE19B799146276Fd8ba7Bf807347A33Ef7Fd49B4b","0x9d88364cE61172D5398cD99c96b8D74899943fF4","0x4e202313790ae15AE84A7E5716EbFbB358C43530","0xE8C5201f0f4bdde271ACC9F14057CB4fe0DBA616","0x0000000000000000000000000000000000000000")))

DROP TABLE IF EXISTS tx_counts;
CREATE TABLE tx_counts (
  type VARCHAR(16),
  token0 VARCHAR(44),
  token1 VARCHAR(44),
  user VARCHAR(44),
  amount INT,
  count INT,
  block_number INT,
  block_time DATETIME,
  time_unit INT,
  time_slot INT,
  updated_time TIMESTAMP,

  PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC, user ASC),
  INDEX (time_unit ASC, type ASC, token0 ASC, token1 ASC, user ASC, time_slot DESC),
  INDEX (time_unit ASC, user ASC, type ASC, token0 ASC, token1 ASC, time_slot DESC),
  -- to get the latest block number
  INDEX (block_number DESC),
  -- for trimming
  INDEX (time_unit ASC, block_time DESC)
);

DROP TABLE IF EXISTS user_counts;
CREATE TABLE user_counts (
  type VARCHAR(16),
  token0 VARCHAR(44),
  token1 VARCHAR(44),
  count INT,
  block_number INT,
  block_time DATETIME,
  time_unit INT,
  time_slot INT,
  updated_time TIMESTAMP,

  PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC),
  INDEX (time_unit ASC, type ASC, token0 ASC, token1 ASC, time_slot DESC),
  -- to get the latest block number
  INDEX (block_number DESC),
  -- for trimming
  INDEX (time_unit ASC, block_time DESC)
);

DROP PROCEDURE IF exists xxx_proc;
DELIMITER //
create procedure xxx_proc()
begin
    declare ix,
    jx INT DEFAULT 0;
    set @ooo = "abc";
    set ix = 0;
    while ix < 4 do
        set ix = ix + 1;
    end while;
end //
delimiter ;


DROP PROCEDURE IF exists xxx_i;
DELIMITER //
create procedure xxx_i()
begin
    Call xxx_ii(200, @_oo);
    SELECT "zzzz", @_oo;
    SET @_oo2 = @_oo;
end //
delimiter ;

DROP PROCEDURE IF exists xxx_ii;
DELIMITER //
create procedure xxx_ii(IN x1 INT, OUT x2 INT)
begin
    SET x2 = x1 + 1;
    SELECT "xxx", x1, x2;
end //
delimiter ;


DROP PROCEDURE IF EXISTS tx_counts_update;
DELIMITER ///
CREATE PROCEDURE tx_counts_update(type_ VARCHAR(16), token_ VARCHAR(44), user_ VARCHAR(44), amount_ INT, count_ INT, block_number_ INT, block_time_ INT, time_unit_ INT, time_slot_ INT)
BEGIN
    DECLARE cnt INT;
    DECLARE amnt INT;
    DECLARE t_type VARCHAR(16);
    DECLARE t_token VARCHAR(44);
    DECLARE t_user VARCHAR(44);
    DECLARE t_unit INT;
    DECLARE t_slot INT;
    DECLARE ix, jx INT;

    -- type = ' ', token = ' ', user = ' '
    -- type = type, token = token, user = ' '
    -- type = ' ', token = ' ', user = user
    -- type = type, token = token, user = user

    -- type = ' ', token = ' ', user = ' ', time_unit = 0, time_slot = 0
    -- type = ' ', token = ' ', user = ' ', time_unit = 1, time_slot = time
    -- type = ' ', token = ' ', user = ' ', time_unit = 60, time_slot = time
    -- type = ' ', token = ' ', user = ' ', time_unit = 3600, time_slot = time
    -- type = ' ', token = ' ', user = ' ', time_unit = 86400, time_slot = time

    SET ix = 0;
    WHILE ix < 4 DO
        IF ix = 0 THEN
            SET t_type = ' ', t_token = ' ', t_user = ' ';
        ELSEIF ix = 1 THEN
            SET t_type = type_, t_token = token_, t_user = ' ';
        ELSEIF ix = 2 THEN
            SET t_type = ' ', t_token = ' ', t_user = user_;
        ELSE
            SET t_type = type_, t_token = token_, t_user = user_;
        END IF;
        SET ix = ix + 1;

        SET jx = 0;
        WHILE jx < 5 DO
            IF jx = 0 THEN
                SET t_unit = 0, t_slot = 0;
            ELSEIF jx = 1 THEN
                SET t_unit = 1, t_slot = UNIX_TIMESTAMP(block_time_);
            ELSEIF jx = 2 THEN
                SET t_unit = 60, t_slot = UNIX_TIMESTAMP(block_time_) DIV 60 * 60;
            ELSEIF jx = 3 THEN
                SET t_unit = 3600, t_slot = UNIX_TIMESTAMP(block_time_) DIV 3600 * 3600;
            ELSE
                SET t_unit = 86400, t_slot = UNIX_TIMESTAMP(block_time_) DIV 86400 * 86400;
            END IF;
            SET jx = jx + 1;

            SET cnt = 0, amnt = 0;
            SELECT count, amount INTO cnt, amnt FROM tx_counts
                WHERE time_unit = t_unit AND time_slot = t_slot AND
	            type = t_type AND token = t_token AND user = t_user;
            IF cnt = 0 THEN
                REPLACE INTO tx_counts SET type = t_type, token = t_token,
                    user = t_user, amount = amnt + amount_, count = cnt + count_,
                    block_number = block_number_, block_time = block_time_,
                    time_unit = t_unit, time_slot = t_slot, updated_time = NOW();
            ELSE
                UPDATE tx_counts SET amount = amnt + amount_,
                    count = cnt + count_, block_number = block_number_,
                    block_time = block_time_, updated_time = NOW()
                    WHERE time_unit = t_unit AND time_slot = t_slot AND
	                type = t_type AND token = t_token AND user = t_user;
            END IF;
        END WHILE;
    END WHILE;
END ///
DELIMITER ;

DROP PROCEDURE IF EXISTS tx_counts_trim;
DELIMITER ///
CREATE PROCEDURE tx_counts_trim()
BEGIN
    DECLARE t_last INT;
    DECLARE duration_seconds INT DEFAULT 3600;
    DECLARE duration_minutes INT DEFAULT 86400;
    DECLARE duration_hours   INT DEFAULT 2678400;
    SELECT FLOOR(UNIX_TIMESTAMP(block_time)) INTO t_last
        FROM tx_counts ORDER BY block_number DESC LIMIT 1,1;
    DELETE FROM tx_counts WHERE time_unit = 1 AND
        block_time < FROM_UNIXTIME(t_last - duration_seconds);
    DELETE FROM tx_counts WHERE time_unit = 60 AND
        block_time < FROM_UNIXTIME(t_last - duration_minutes);
    DELETE FROM tx_counts WHERE time_unit = 3600 AND
        block_time < FROM_UNIXTIME(t_last - duration_hours);
END ///
DELIMITER ;

DROP PROCEDURE IF EXISTS tx_counts_update;
DELIMITER ///
CREATE PROCEDURE tx_counts_update(type_ VARCHAR(16), token_ VARCHAR(44), user_ VARCHAR(44), amount_ INT, count_ INT, block_number_ INT, block_time_ TIMESTAMP)
BEGIN
    DECLARE n1_0, n1_1, n1_60, n1_3600, n1_86400,
            n2_0, n2_1, n2_60, n2_3600, n2_86400,
            n3_0, n3_1, n3_60, n3_3600, n3_86400,
            n4_0, n4_1, n4_60, n4_3600, n4_86400,
            m1_0, m1_1, m1_60, m1_3600, m1_86400,
            m2_0, m2_1, m2_60, m2_3600, m2_86400,
            m3_0, m3_1, m3_60, m3_3600, m3_86400,
            m4_0, m4_1, m4_60, m4_3600, m4_86400 INT DEFAULT 0;
    DECLARE num INT DEFAULT block_number_;
    DECLARE tim TIMESTAMP DEFAULT block_time_;
    DECLARE now TIMESTAMP DEFAULT NOW();
    DECLARE ts1 INT DEFAULT FLOOR(UNIX_TIMESTAMP(block_time_));
    DECLARE ts60 INT DEFAULT ts1 DIV 60 * 60;
    DECLARE ts3600 INT DEFAULT ts1 DIV 3600 * 3600;
    DECLARE ts86400 INT DEFAULT ts1 DIV 86400 * 86400;

    SELECT count, amount INTO n1_0, m1_0 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n1_1, m1_1 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n1_60, m1_60 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n1_3600, m1_3600 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n1_86400, m1_86400 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    SELECT count, amount INTO n2_0, m2_0 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n2_1, m2_1 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n2_60, m2_60 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n2_3600, m2_3600 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n2_86400, m2_86400 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    SELECT count, amount INTO n3_0, m3_0 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = user_ AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n3_1, m3_1 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = user_ AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n3_60, m3_60 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = user_ AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n3_3600, m3_3600 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = user_ AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n3_86400, m3_86400 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = user_ AND time_unit = 86400 AND time_slot = ts86400;

    SELECT count, amount INTO n4_0, m4_0 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = user_ AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n4_1, m4_1 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = user_ AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n4_60, m4_60 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = user_ AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n4_3600, m4_3600 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = user_ AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n4_86400, m4_86400 FROM tx_counts WHERE
type = type_ AND token = token_ AND user = user_ AND time_unit = 86400 AND time_slot = ts86400;

    REPLACE INTO tx_counts (type, token, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES
    (' ', ' ', ' ', amount_ + m1_0,     count_ + n1_0,     num, tim, 0,     0, now),
    (' ', ' ', ' ', amount_ + m1_1,     count_ + n1_1,     num, tim, 1,     ts1, now),
    (' ', ' ', ' ', amount_ + m1_60,    count_ + n1_60,    num, tim, 60,    ts60, now),
    (' ', ' ', ' ', amount_ + m1_3600,  count_ + n1_3600,  num, tim, 3600,  ts3600, now),
    (' ', ' ', ' ', amount_ + m1_86400, count_ + n1_86400, num, tim, 86400, ts86400, now),

    (type_, token_, ' ', amount_ + m2_0,     count_ + n2_0,     num, tim, 0,     0, now),
    (type_, token_, ' ', amount_ + m2_1,     count_ + n2_1,     num, tim, 1,     ts1, now),
    (type_, token_, ' ', amount_ + m2_60,    count_ + n2_60,    num, tim, 60,    ts60, now),
    (type_, token_, ' ', amount_ + m2_3600,  count_ + n2_3600,  num, tim, 3600,  ts3600, now),
    (type_, token_, ' ', amount_ + m2_86400, count_ + n2_86400, num, tim, 86400, ts86400, now),

    (' ', ' ', user_, amount_ + m3_0,     count_ + n3_0,     num, tim, 0,     0, now),
    (' ', ' ', user_, amount_ + m3_1,     count_ + n3_1,     num, tim, 1,     ts1, now),
    (' ', ' ', user_, amount_ + m3_60,    count_ + n3_60,    num, tim, 60,    ts60, now),
    (' ', ' ', user_, amount_ + m3_3600,  count_ + n3_3600,  num, tim, 3600,  ts3600, now),
    (' ', ' ', user_, amount_ + m3_86400, count_ + n3_86400, num, tim, 86400, ts86400, now),

    (type_, token_, user_, amount_ + m4_0,     count_ + n4_0,     num, tim, 0,     0, now),
    (type_, token_, user_, amount_ + m4_1,     count_ + n4_1,     num, tim, 1,     ts1, now),
    (type_, token_, user_, amount_ + m4_60,    count_ + n4_60,    num, tim, 60,    ts60, now),
    (type_, token_, user_, amount_ + m4_3600,  count_ + n4_3600,  num, tim, 3600,  ts3600, now),
    (type_, token_, user_, amount_ + m4_86400, count_ + n4_86400, num, tim, 86400, ts86400, now);

END ///
DELIMITER ;

-- for TxHistory
DROP PROCEDURE IF EXISTS tx_counts_update_row;
DELIMITER ///
CREATE PROCEDURE tx_counts_update_row(_tx_hash VARCHAR(66))
doit: BEGIN
    DECLARE _type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amount, _count INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE n1_0, n1_1, n1_60, n1_3600, n1_86400,
            n2_0, n2_1, n2_60, n2_3600, n2_86400,
            n3_0, n3_1, n3_60, n3_3600, n3_86400,
            n4_0, n4_1, n4_60, n4_3600, n4_86400,
            m1_0, m1_1, m1_60, m1_3600, m1_86400,
            m2_0, m2_1, m2_60, m2_3600, m2_86400,
            m3_0, m3_1, m3_60, m3_3600, m3_86400,
            m4_0, m4_1, m4_60, m4_3600, m4_86400 INT DEFAULT 0;
    DECLARE now TIMESTAMP DEFAULT NOW();
    DECLARE ts1, ts60, ts3600, ts86400 INT;

    SELECT type, token0, token1, user, block_number, block_timestamp
      INTO _type, _token0, _token1, _user, _bnum, _btime
--         FROM TxHistory WHERE tx_hash = tx_hash LIMIT 1,1;
         FROM txhistory WHERE tx_hash = _tx_hash LIMIT 1,1;
    IF ISNULL(_type) or ISNULL(_btime) THEN
        LEAVE doit;
    END IF;

    SET ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
        ts60 = ts1 DIV 60 * 60,
        ts3600 = ts1 DIV 3600 * 3600,
        ts86400 = ts1 DIV 86400 * 86400;

    SELECT count, amount INTO n1_0, m1_0 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n1_1, m1_1 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n1_60, m1_60 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n1_3600, m1_3600 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n1_86400, m1_86400 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    SELECT count, amount INTO n2_0, m2_0 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n2_1, m2_1 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n2_60, m2_60 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n2_3600, m2_3600 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n2_86400, m2_86400 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    SELECT count, amount INTO n3_0, m3_0 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n3_1, m3_1 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n3_60, m3_60 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n3_3600, m3_3600 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n3_86400, m3_86400 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 86400 AND time_slot = ts86400;

    SELECT count, amount INTO n4_0, m4_0 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 0 AND time_slot = 0;
    SELECT count, amount INTO n4_1, m4_1 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 1 AND time_slot = ts1;
    SELECT count, amount INTO n4_60, m4_60 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 60 AND time_slot = ts60;
    SELECT count, amount INTO n4_3600, m4_3600 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, amount INTO n4_86400, m4_86400 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 86400 AND time_slot = ts86400;

    REPLACE INTO tx_counts (type, token, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES
    (' ', ' ', ' ', _amount + m1_0,     _count + n1_0,     _bnum, _btime, 0,     0, now),
    (' ', ' ', ' ', _amount + m1_1,     _count + n1_1,     _bnum, _btime, 1,     ts1, now),
    (' ', ' ', ' ', _amount + m1_60,    _count + n1_60,    _bnum, _btime, 60,    ts60, now),
    (' ', ' ', ' ', _amount + m1_3600,  _count + n1_3600,  _bnum, _btime, 3600,  ts3600, now),
    (' ', ' ', ' ', _amount + m1_86400, _count + n1_86400, _bnum, _btime, 86400, ts86400, now),

    (_type, _token0, ' ', _amount + m2_0,     _count + n2_0,     _bnum, _btime, 0,     0, now),
    (_type, _token0, ' ', _amount + m2_1,     _count + n2_1,     _bnum, _btime, 1,     ts1, now),
    (_type, _token0, ' ', _amount + m2_60,    _count + n2_60,    _bnum, _btime, 60,    ts60, now),
    (_type, _token0, ' ', _amount + m2_3600,  _count + n2_3600,  _bnum, _btime, 3600,  ts3600, now),
    (_type, _token0, ' ', _amount + m2_86400, _count + n2_86400, _bnum, _btime, 86400, ts86400, now),

    (' ', ' ', _user, _amount + m3_0,     _count + n3_0,     _bnum, _btime, 0,     0, now),
    (' ', ' ', _user, _amount + m3_1,     _count + n3_1,     _bnum, _btime, 1,     ts1, now),
    (' ', ' ', _user, _amount + m3_60,    _count + n3_60,    _bnum, _btime, 60,    ts60, now),
    (' ', ' ', _user, _amount + m3_3600,  _count + n3_3600,  _bnum, _btime, 3600,  ts3600, now),
    (' ', ' ', _user, _amount + m3_86400, _count + n3_86400, _bnum, _btime, 86400, ts86400, now),

    (_type, _token0, _user, _amount + m4_0,     _count + n4_0,     _bnum, _btime, 0,     0, now),
    (_type, _token0, _user, _amount + m4_1,     _count + n4_1,     _bnum, _btime, 1,     ts1, now),
    (_type, _token0, _user, _amount + m4_60,    _count + n4_60,    _bnum, _btime, 60,    ts60, now),
    (_type, _token0, _user, _amount + m4_3600,  _count + n4_3600,  _bnum, _btime, 3600,  ts3600, now),
    (_type, _token0, _user, _amount + m4_86400, _count + n4_86400, _bnum, _btime, 86400, ts86400, now);

END ///
DELIMITER ;


-- for txhistory
DROP PROCEDURE IF EXISTS tx_counts_update_row;
DELIMITER ///
CREATE PROCEDURE tx_counts_update_row(_tx_hash VARCHAR(66))
doit: BEGIN
    DECLARE _type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amount, _count INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE n1_0, n1_1, n1_60, n1_3600, n1_86400,
            n2_0, n2_1, n2_60, n2_3600, n2_86400,
            n3_0, n3_1, n3_60, n3_3600, n3_86400,
            n4_0, n4_1, n4_60, n4_3600, n4_86400,
            m1_0, m1_1, m1_60, m1_3600, m1_86400,
            m2_0, m2_1, m2_60, m2_3600, m2_86400,
            m3_0, m3_1, m3_60, m3_3600, m3_86400,
            m4_0, m4_1, m4_60, m4_3600, m4_86400,
            m5_0, m5_1, m5_60, m5_3600, m5_86400,
            m6_0, m6_1, m6_60, m6_3600, m6_86400 INT DEFAULT 0;
    DECLARE now TIMESTAMP DEFAULT NOW();
    DECLARE ts1, ts60, ts3600, ts86400 INT;

    SELECT type, token0, token1, user, block_number, block_timestamp
      INTO _type, _token0, _token1, _user, _bnum, _btime
         FROM txhistory WHERE tx_hash = _tx_hash;
    IF ISNULL(_type) or ISNULL(_btime) THEN
        LEAVE doit;
    END IF;

    SET ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
        ts60 = ts1 DIV 60 * 60,
        ts3600 = ts1 DIV 3600 * 3600,
        ts86400 = ts1 DIV 86400 * 86400;

    -- type = ' ', token = ' ', user = ' '
    SELECT count, count INTO n1_0, m1_0 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, count INTO n1_1, m1_1 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, count INTO n1_60, m1_60 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, count INTO n1_3600, m1_3600 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, count INTO n1_86400, m1_86400 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    -- type = <type>, token = ' ', user = ' '
    SELECT count, count INTO n2_0, m2_0 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, count INTO n2_1, m2_1 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, count INTO n2_60, m2_60 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, count INTO n2_3600, m2_3600 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, count INTO n2_86400, m2_86400 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    -- type = <type>, token = <token>, user = ' '
    SELECT count, count INTO n3_0, m3_0 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 0 AND time_slot = 0;
    SELECT count, count INTO n3_1, m3_1 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 1 AND time_slot = ts1;
    SELECT count, count INTO n3_60, m3_60 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 60 AND time_slot = ts60;
    SELECT count, count INTO n3_3600, m3_3600 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, count INTO n3_86400, m3_86400 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = ' ' AND time_unit = 86400 AND time_slot = ts86400;

    -- type = ' ', token = ' ', user = <user>
    SELECT count, count INTO n4_0, m4_0 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 0 AND time_slot = 0;
    SELECT count, count INTO n4_1, m4_1 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 1 AND time_slot = ts1;
    SELECT count, count INTO n4_60, m4_60 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 60 AND time_slot = ts60;
    SELECT count, count INTO n4_3600, m4_3600 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, count INTO n4_86400, m4_86400 FROM tx_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 86400 AND time_slot = ts86400;

    -- type = <type>, token = ' ', user = <user>
    SELECT count, count INTO n5_0, m5_0 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = _user AND time_unit = 0 AND time_slot = 0;
    SELECT count, count INTO n5_1, m5_1 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = _user AND time_unit = 1 AND time_slot = ts1;
    SELECT count, count INTO n5_60, m5_60 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = _user AND time_unit = 60 AND time_slot = ts60;
    SELECT count, count INTO n5_3600, m5_3600 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = _user AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, count INTO n5_86400, m5_86400 FROM tx_counts WHERE
type = _type AND token = ' ' AND user = _user AND time_unit = 86400 AND time_slot = ts86400;

    -- type = <type>, token = <token>, user = <user>
    SELECT count, count INTO n6_0, m6_0 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 0 AND time_slot = 0;
    SELECT count, count INTO n6_1, m6_1 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 1 AND time_slot = ts1;
    SELECT count, count INTO n6_60, m6_60 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 60 AND time_slot = ts60;
    SELECT count, count INTO n6_3600, m6_3600 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 3600 AND time_slot = ts3600;
    SELECT count, count INTO n6_86400, m6_86400 FROM tx_counts WHERE
type = _type AND token = _token0 AND user = _user AND time_unit = 86400 AND time_slot = ts86400;

    REPLACE INTO tx_counts (type, token, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES
    (' ', ' ', ' ', _amount + m1_0,     _count + n1_0,     _bnum, _btime, 0,     0, now),
    (' ', ' ', ' ', _amount + m1_1,     _count + n1_1,     _bnum, _btime, 1,     ts1, now),
    (' ', ' ', ' ', _amount + m1_60,    _count + n1_60,    _bnum, _btime, 60,    ts60, now),
    (' ', ' ', ' ', _amount + m1_3600,  _count + n1_3600,  _bnum, _btime, 3600,  ts3600, now),
    (' ', ' ', ' ', _amount + m1_86400, _count + n1_86400, _bnum, _btime, 86400, ts86400, now),

    (_type, ' ', ' ', _amount + m2_0,     _count + n2_0,     _bnum, _btime, 0,     0, now),
    (_type, ' ', ' ', _amount + m2_1,     _count + n2_1,     _bnum, _btime, 1,     ts1, now),
    (_type, ' ', ' ', _amount + m2_60,    _count + n2_60,    _bnum, _btime, 60,    ts60, now),
    (_type, ' ', ' ', _amount + m2_3600,  _count + n2_3600,  _bnum, _btime, 3600,  ts3600, now),
    (_type, ' ', ' ', _amount + m2_86400, _count + n2_86400, _bnum, _btime, 86400, ts86400, now),

    (_type, _token0, ' ', _amount + m3_0,     _count + n3_0,     _bnum, _btime, 0,     0, now),
    (_type, _token0, ' ', _amount + m3_1,     _count + n3_1,     _bnum, _btime, 1,     ts1, now),
    (_type, _token0, ' ', _amount + m3_60,    _count + n3_60,    _bnum, _btime, 60,    ts60, now),
    (_type, _token0, ' ', _amount + m3_3600,  _count + n3_3600,  _bnum, _btime, 3600,  ts3600, now),
    (_type, _token0, ' ', _amount + m3_86400, _count + n3_86400, _bnum, _btime, 86400, ts86400, now),

    (' ', ' ', _user, _amount + m4_0,     _count + n4_0,     _bnum, _btime, 0,     0, now),
    (' ', ' ', _user, _amount + m4_1,     _count + n4_1,     _bnum, _btime, 1,     ts1, now),
    (' ', ' ', _user, _amount + m4_60,    _count + n4_60,    _bnum, _btime, 60,    ts60, now),
    (' ', ' ', _user, _amount + m4_3600,  _count + n4_3600,  _bnum, _btime, 3600,  ts3600, now),
    (' ', ' ', _user, _amount + m4_86400, _count + n4_86400, _bnum, _btime, 86400, ts86400, now),

    (_type, ' ', _user, _amount + m5_0,     _count + n5_0,     _bnum, _btime, 0,     0, now),
    (_type, ' ', _user, _amount + m5_1,     _count + n5_1,     _bnum, _btime, 1,     ts1, now),
    (_type, ' ', _user, _amount + m5_60,    _count + n5_60,    _bnum, _btime, 60,    ts60, now),
    (_type, ' ', _user, _amount + m5_3600,  _count + n5_3600,  _bnum, _btime, 3600,  ts3600, now),
    (_type, ' ', _user, _amount + m5_86400, _count + n5_86400, _bnum, _btime, 86400, ts86400, now),

    (_type, _token0, _user, _amount + m6_0,     _count + n6_0,     _bnum, _btime, 0,     0, now),
    (_type, _token0, _user, _amount + m6_1,     _count + n6_1,     _bnum, _btime, 1,     ts1, now),
    (_type, _token0, _user, _amount + m6_60,    _count + n6_60,    _bnum, _btime, 60,    ts60, now),
    (_type, _token0, _user, _amount + m6_3600,  _count + n6_3600,  _bnum, _btime, 3600,  ts3600, now),
    (_type, _token0, _user, _amount + m6_86400, _count + n6_86400, _bnum, _btime, 86400, ts86400, now);

    -- user counts
    -- _type, _token, count += 1, _bnum, _btime, 0, 0, now
    IF n4_0 = 0 THEN
        SELECT count INTO o4_0 FROM user_counts WHERE
type = ' ' AND token = ' ' AND user = _user AND time_unit = 0 AND time_slot = 0;
    END IF




END ///
DELIMITER ;

0x000065d2e6244089000d340581d0cdf6534377f8d6737b7cc7a65982fb80a705
0x000074d79662309c96ff57fd9dd558468323e842760c49627bced6ac9ecd9b54
0x000088eb66eb745f1cba9b3e6421efb8ac7ea025c9f7a0e960fe3c3587894f76
0x0000bd73bafa0c3d04467b760fd5eaea77df0b234a4651811faafa7c9c7550e9
0x000136de0c046738e8da2bf24a07002d1f37f60992d9e406e86943a99376d851
0x0001b4a4f87f6a0f34a0c83a423586df283b022b6e5b4ede6343d581e641a5ab
0x0001cdfe6448a7de9ce1563f37d8be897fcaa3784120daef487383e48e7ce901
0x00026bf77c139d91f92e1a8c8330ba58c8472228190327bfb8004dd341c2b4ad
0x0002b5f630d7f4bafb83e6a55ed84991c47688d05d76ab890a91287c15a5f4fb
0x0002ef8759e6e1961463d08d26c729b3af534dfbe84ba2bf4d45d33a768f5907
-- for txhistory, tx_counts and user_counts
-- * type, token, user
--   0: ' '   ' '    ' '      -> total count
--   1: type  ' '    ' '      -> total for the type
--   2: type  token  ' '      -> total for the type and token
--   3: ' '   ' '    user     -> total for the user
--   4: type  ' '    user     -> total for the type and user
--   5: type  token  user     -> total for the type, token and user
-- * time series
--   unit, slot,  instances
--   0      0     1        -> total count, only 1 instance
--   1      time  3600     -> seconds for an hour, 3600 instances
--   60     time  1440     -> minutes for a day (86400 seconds), 1440 instances
--   3600   time  744      -> hours for a month (31 days, 2678400 seconds), 744
--   87400  time  -        -> days forever
-- * # of rows
--   (#types + 1) * (#token + 1) * (#user + 1) * (1 + 3600 + 86400 + 744 + <days)
DROP PROCEDURE IF EXISTS tx_counts_update_row;
DELIMITER ///
CREATE PROCEDURE tx_counts_update_row(_tx_hash VARCHAR(66))
doit: BEGIN
    DECLARE _type, __type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amt, _cnt INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE _now TIMESTAMP DEFAULT NOW();
    DECLARE _ts1, _ts60, _ts3600, _ts86400 INT;
    DECLARE __token0, __token1, __user VARCHAR(44);
    DECLARE __amt, __cnt, __tunit, __tslot INT;
    DECLARE _i, _j INT;
    DECLARE _ucnt INT DEFAULT 0;
    SET @_txs_stmt = "", @_usr_stmt = "";

    SELECT type, token0, token1, user, block_number, block_timestamp
      INTO _type, _token0, _token1, _user, _bnum, _btime
         FROM txhistory WHERE tx_hash = _tx_hash;
    IF ISNULL(_type) or ISNULL(_btime) THEN
        LEAVE doit;
    END IF;

    -- TODO: ts86400 needs to use TIMEZONE, it's in UTC for now
    SET _ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
        _ts60 = _ts1 DIV 60 * 60,
        _ts3600 = _ts1 DIV 3600 * 3600,
        _ts86400 = _ts1 DIV 86400 * 86400;

    SET _i = 0;
    WHILE _i < 6 DO
        IF _i = 0 THEN
            SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = ' ';
        ELSEIF _i = 1 THEN
            SET __type = _type, __token0 = ' ', __token1 = ' ', __user = ' ';
        ELSEIF _i = 2 THEN
            SET __type = _type, __token0 = _token0, __token1 = _token1, __user = ' ';
        ELSEIF _i = 3 THEN
            SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = _user;
        ELSEIF _i = 4 THEN
            SET __type = _type, __token0 = ' ', __token1 = ' ', __user = _user;
        ELSE
            SET __type = _type, __token0 = _token0, __token1 = _token1, __user = _user;
        END IF;
        SET _i = _i + 1;

        SET _j = 0;
        WHILE _j < 5 DO
            IF _j = 0 THEN
                SET __tunit = 0, __tslot = 0;
            ELSEIF _j = 1 THEN
                SET __tunit = 1, __tslot = _ts1;
            ELSEIF _j = 2 THEN
                SET __tunit = 60, __tslot = _ts60;
            ELSEIF _j = 3 THEN
                SET __tunit = 3600, __tslot = _ts3600;
            ELSE
                SET __tunit = 86400, __tslot = _ts86400;
            END IF;
            SET _j = _j + 1;

            -- tx_counts table
            SET __cnt = 0, __amt = 0;
            SELECT count, count INTO __cnt, __amt FROM tx_counts WHERE
                type = __type AND token = __token0 AND user = __user AND
                time_unit = __tunit AND time_slot = __tslot;

            IF (NOT _j = 1) OR (NOT _i = 1) THEN
                SET @_txs_stmt = CONCAT(@_txs_stmt, ",");
            END IF;
            SET @_txs_stmt = CONCAT(@_txs_stmt, "('", __type, "','",
                    __token0, "','", __user, "',", __amt + _amt, ",",
                    __cnt + _cnt, ",", _bnum, ",'", _btime, "',",
                    __tunit, ",", __tslot, ",'", _now, "')");

            -- user_counts table
            IF (NOT __user = ' ') AND __cnt = 0 THEN
                SET __cnt = 0;
                SELECT count INTO __cnt FROM user_counts WHERE
                    type = __type AND token = __token0 AND
                    time_unit = __tunit AND time_slot = __tslot;

                IF NOT _ucnt = 0 THEN
                    SET @_usr_stmt = CONCAT(@_usr_stmt, ",");
                END IF;
                SET @_usr_stmt = CONCAT(@_usr_stmt, "('", __type, "','",
                        __token0, "',", __cnt + 1, ",", _bnum, ",'",
                        _btime, "',", __tunit, ",", __tslot, ",'", _now, "')");
                SET _ucnt = _ucnt + 1;
            END IF;
        END WHILE;
    END WHILE;

    SET @_txs_stmt = CONCAT("REPLACE INTO tx_counts (type, token, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
        @_txs_stmt);
    PREPARE stmt FROM @_txs_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    IF _ucnt > 0 THEN
        SET @_usr_stmt = CONCAT("REPLACE INTO user_counts (type, token, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
            @_usr_stmt);
        PREPARE stmt FROM @_usr_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    -- SET @_txs_stmt = NULL, @_usr_stmt = NULL;
END ///
DELIMITER ;


DROP PROCEDURE IF EXISTS tx_counts_trim;
DELIMITER ///
CREATE PROCEDURE tx_counts_trim()
BEGIN
    DECLARE t_last INT;
    DECLARE duration_seconds INT DEFAULT 3600;
    DECLARE duration_minutes INT DEFAULT 86400;
    DECLARE duration_hours   INT DEFAULT 2678400;
    SELECT FLOOR(UNIX_TIMESTAMP(block_time)) INTO t_last
        FROM tx_counts ORDER BY block_number DESC LIMIT 1,1;
    DELETE FROM tx_counts WHERE time_unit = 1 AND
        block_time < FROM_UNIXTIME(t_last - duration_seconds);
    DELETE FROM tx_counts WHERE time_unit = 60 AND
        block_time < FROM_UNIXTIME(t_last - duration_minutes);
    DELETE FROM tx_counts WHERE time_unit = 3600 AND
        block_time < FROM_UNIXTIME(t_last - duration_hours);
    DELETE FROM user_counts WHERE time_unit = 1 AND
        block_time < FROM_UNIXTIME(t_last - duration_seconds);
    DELETE FROM user_counts WHERE time_unit = 60 AND
        block_time < FROM_UNIXTIME(t_last - duration_minutes);
    DELETE FROM user_counts WHERE time_unit = 3600 AND
        block_time < FROM_UNIXTIME(t_last - duration_hours);
END ///
DELIMITER ;

-- UNIX_TIMESTAMP(xxx) <-> FROM_UNIXTIME(111)
START TRANSACTION;
REPLACE INTO tx_counts SET type = 'x', token = 'x', user = 'x', amount = 111,
    count = 111, block_number = 1, block_time = FROM_UNIXTIME(11), time_unit = 1, time_slot = 1,
    updated_time = NOW();
REPLACE INTO tx_counts SET type = 'x', token = 'x', amount = 222,
    count = 222, block_number = 1, block_time = FROM_UNIXTIME(11), time_unit = 1, time_slot = 1,
    updated_time = NOW();
ROLLBACK;
COMMIT;


-- to use cursor
DROP PROCEDURE IF EXISTS tx_counts_proc_blocks;
DELIMITER ///
CREATE PROCEDURE tx_counts_proc_blocks(_start INT, _end INT)
BEGIN
    DECLARE _done INT DEFAULT 0;
    DECLARE _type, __type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amt, _cnt INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE _now TIMESTAMP DEFAULT NOW();
    DECLARE _ts1, _ts60, _ts3600, _ts86400 INT;
    DECLARE __token0, __token1, __user VARCHAR(44);
    DECLARE __amt, __cnt, __tunit, __tslot INT;
    DECLARE _i, _j INT;
    DECLARE _ucnt, _nproc INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT type, token0, token1, user, block_number, block_timestamp
            FROM txhistory WHERE
                _start <= block_number AND (_end <= 0 OR block_number <= _end)
            ORDER BY block_number ASC;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _done = 1;

    -- need to specify indexes including primary key
    CREATE TEMPORARY TABLE _tx_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token ASC, user ASC),
         INDEX (time_unit ASC, type ASC, token ASC, user ASC, time_slot DESC),
         INDEX (block_number DESC),
         INDEX (time_unit ASC, block_time DESC))
        SELECT * FROM tx_counts LIMIT 0;
    CREATE TEMPORARY TABLE _user_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token ASC),
         INDEX (time_unit ASC, type ASC, token ASC, time_slot DESC),
         INDEX (block_number DESC),
         INDEX (time_unit ASC, block_time DESC))
        SELECT * FROM user_counts LIMIT 0;

    OPEN cur;
    iter: LOOP
        SET _done = 0;
        FETCH cur INTO _type, _token0, _token1, _user, _bnum, _btime;
        IF _done = 1 THEN
            LEAVE iter;
        END IF;
        IF ISNULL(_btime) THEN
            ITERATE iter;
        END IF;

        SET @_txs_stmt = "", @_usr_stmt = "";

        -- TODO: ts86400 needs to use TIMEZONE, it's in UTC for now
        SET _ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
            _ts60 = _ts1 DIV 60 * 60,
            _ts3600 = _ts1 DIV 3600 * 3600,
            _ts86400 = _ts1 DIV 86400 * 86400;
    
        SET _i = 0, _ucnt = 0;
        WHILE _i < 6 DO
            IF _i = 0 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 1 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 2 THEN
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = ' ';
            ELSEIF _i = 3 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = _user;
            ELSEIF _i = 4 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = _user;
            ELSE
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = _user;
            END IF;
            SET _i = _i + 1;
    
            SET _j = 0;
            WHILE (_i < 3 AND _j < 5) OR (3 <= _i AND _j = 0) DO
                IF _j = 0 THEN
                    SET __tunit = 0, __tslot = 0;
                ELSEIF _j = 1 THEN
                    SET __tunit = 1, __tslot = _ts1;
                ELSEIF _j = 2 THEN
                    SET __tunit = 60, __tslot = _ts60;
                ELSEIF _j = 3 THEN
                    SET __tunit = 3600, __tslot = _ts3600;
                ELSE
                    SET __tunit = 86400, __tslot = _ts86400;
                END IF;
                SET _j = _j + 1;
    
                -- tx_counts table
                -- from temporary table, then if not present, from tx_counts 
                SET __cnt = 0, __amt = 0;
                SELECT count, count INTO __cnt, __amt FROM _tx_counts WHERE
                    type = __type AND token = __token0 AND user = __user AND
                    time_unit = __tunit AND time_slot = __tslot;
                IF __cnt = 0 THEN
                    SELECT count, count INTO __cnt, __amt FROM tx_counts WHERE
                        type = __type AND token = __token0 AND user = __user AND
                        time_unit = __tunit AND time_slot = __tslot;
                END IF;
    
                IF (NOT _j = 1) OR (NOT _i = 1) THEN
                    SET @_txs_stmt = CONCAT(@_txs_stmt, ",");
                END IF;
                SET @_txs_stmt = CONCAT(@_txs_stmt, "('", __type, "','",
                        __token0, "','", __user, "',", __amt + _amt, ",",
                        __cnt + _cnt, ",", _bnum, ",'", _btime, "',",
                        __tunit, ",", __tslot, ",'", _now, "')");
    
                -- user_counts table
                IF (NOT __user = ' ') AND __cnt = 0 THEN
                    -- from temporary table, then if not present, from tx_counts 
                    SET __cnt = 0;
                    SELECT count INTO __cnt FROM _user_counts WHERE
                        type = __type AND token = __token0 AND
                        time_unit = __tunit AND time_slot = __tslot;
                    IF __cnt = 0 THEN
                        SELECT count INTO __cnt FROM user_counts WHERE
                            type = __type AND token = __token0 AND
                            time_unit = __tunit AND time_slot = __tslot;
                    END IF;
    
                    IF NOT _ucnt = 0 THEN
                        SET @_usr_stmt = CONCAT(@_usr_stmt, ",");
                    END IF;
                    SET @_usr_stmt = CONCAT(@_usr_stmt, "('", __type, "','",
                            __token0, "',", __cnt + 1, ",", _bnum, ",'",
                            _btime, "',", __tunit, ",", __tslot, ",'", _now, "')");
                    SET _ucnt = _ucnt + 1;
                END IF;
            END WHILE;
        END WHILE;

        SET @_txs_stmt = CONCAT("REPLACE INTO _tx_counts (type, token, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
            @_txs_stmt);
        PREPARE stmt FROM @_txs_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF _ucnt > 0 THEN
            SET @_usr_stmt = CONCAT("REPLACE INTO _user_counts (type, token, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
                @_usr_stmt);
            PREPARE stmt FROM @_usr_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        IF _nproc >= 1000 THEN
            -- flush from the temporary to the real
            REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
            REPLACE INTO user_counts SELECT * FROM _user_counts;
	    -- commit happens here
            -- TRUNCATE _tx_counts;
            -- TRUNCATE _user_counts;
	    DELETE FROM _tx_counts;
	    DELETE FROM _user_counts;

            CALL tx_counts_trim();
            SET _nproc = 0;
        ELSE
            SET _nproc = _nproc + 1;
        END IF;

    END LOOP iter;
    CLOSE cur;

    -- flush from the temporary to the real
    REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
    REPLACE INTO user_counts SELECT * FROM _user_counts;

    DROP TEMPORARY TABLE _tx_counts;
    DROP TEMPORARY TABLE _user_counts;
    -- SET @_txs_stmt = "", @_usr_stmt = "";

END ///
DELIMITER ;

-- with token0 & token1
DROP PROCEDURE IF EXISTS tx_counts_proc_blocks;
DELIMITER ///
CREATE PROCEDURE tx_counts_proc_blocks(_start INT, _end INT)
BEGIN
    DECLARE _done INT DEFAULT 0;
    DECLARE _type, __type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amt, _cnt INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE _now TIMESTAMP DEFAULT NOW();
    DECLARE _ts1, _ts60, _ts3600, _ts86400 INT;
    DECLARE __token0, __token1, __user VARCHAR(44);
    DECLARE __amt, __cnt, __tunit, __tslot INT;
    DECLARE _i, _j INT;
    DECLARE _ucnt, _nproc INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT type, token0, token1, user, block_number, block_timestamp
            FROM txhistory WHERE
                _start <= block_number AND (_end <= 0 OR block_number <= _end)
            ORDER BY block_number ASC;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _done = 1;

    -- need to specify indexes including primary key
    CREATE TEMPORARY TABLE _tx_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC, user ASC))
        SELECT * FROM tx_counts LIMIT 0;
    CREATE TEMPORARY TABLE _user_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1))
        SELECT * FROM user_counts LIMIT 0;

    OPEN cur;
    iter: LOOP
        SET _done = 0;
        FETCH cur INTO _type, _token0, _token1, _user, _bnum, _btime;
        IF _done = 1 THEN
            LEAVE iter;
        END IF;
        IF ISNULL(_btime) THEN
            ITERATE iter;
        END IF;

        SET @_txs_stmt = "", @_usr_stmt = "";

        -- TODO: ts86400 needs to use TIMEZONE, it's in UTC for now
        SET _ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
            _ts60 = _ts1 DIV 60 * 60,
            _ts3600 = _ts1 DIV 3600 * 3600,
            _ts86400 = _ts1 DIV 86400 * 86400;
    
        SET _i = 0, _ucnt = 0;
        WHILE _i < 6 DO
            IF _i = 0 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 1 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 2 THEN
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = ' ';
            ELSEIF _i = 3 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = _user;
            ELSEIF _i = 4 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = _user;
            ELSE
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = _user;
            END IF;
            SET _i = _i + 1;
    
            SET _j = 0;
            WHILE (_i < 3 AND _j < 5) OR (3 <= _i AND _j = 0) DO
                IF _j = 0 THEN
                    SET __tunit = 0, __tslot = 0;
                ELSEIF _j = 1 THEN
                    SET __tunit = 1, __tslot = _ts1;
                ELSEIF _j = 2 THEN
                    SET __tunit = 60, __tslot = _ts60;
                ELSEIF _j = 3 THEN
                    SET __tunit = 3600, __tslot = _ts3600;
                ELSE
                    SET __tunit = 86400, __tslot = _ts86400;
                END IF;
                SET _j = _j + 1;
    
                -- tx_counts table
                -- from temporary table, then if not present, from tx_counts 
                SET __cnt = 0, __amt = 0;
                SELECT count, count INTO __cnt, __amt FROM _tx_counts WHERE
                    type = __type AND token0 = __token0 AND token1 = __token1
	       	    AND user = __user AND time_unit = __tunit
		    AND time_slot = __tslot;
                IF __cnt = 0 THEN
                    SELECT count, count INTO __cnt, __amt FROM tx_counts WHERE
                        type = __type AND token0 = __token0 AND token1 = __token1
			AND user = __user AND time_unit = __tunit
			AND time_slot = __tslot;
                END IF;
    
                IF (NOT _j = 1) OR (NOT _i = 1) THEN
                    SET @_txs_stmt = CONCAT(@_txs_stmt, ",");
                END IF;
                SET @_txs_stmt = CONCAT(@_txs_stmt, "('", __type, "','",
                        __token0, "','", __token1, "','", __user, "',",
		       	__amt + _amt, ",", __cnt + _cnt, ",", _bnum, ",'",
		       	_btime, "',", __tunit, ",", __tslot, ",'", _now, "')");
    
                -- user_counts table
                IF (NOT __user = ' ') AND __cnt = 0 THEN
                    -- from temporary table, then if not present, from tx_counts 
                    SET __cnt = 0;
                    SELECT count INTO __cnt FROM _user_counts WHERE
                        type = __type AND token0 = __token0 AND token1 = __token1
			AND time_unit = __tunit AND time_slot = __tslot;
                    IF __cnt = 0 THEN
                        SELECT count INTO __cnt FROM user_counts WHERE
                            type = __type AND token0 = __token0 AND token1 = __token1
			    AND time_unit = __tunit AND time_slot = __tslot;
                    END IF;
    
                    IF NOT _ucnt = 0 THEN
                        SET @_usr_stmt = CONCAT(@_usr_stmt, ",");
                    END IF;
                    SET @_usr_stmt = CONCAT(@_usr_stmt, "('", __type, "','",
                            __token0, "','", __token1, "',", __cnt + 1, ",",
			    _bnum, ",'", _btime, "',", __tunit, ",",
			    __tslot, ",'", _now, "')");
                    SET _ucnt = _ucnt + 1;
                END IF;
            END WHILE;
        END WHILE;

        SET @_txs_stmt = CONCAT("REPLACE INTO _tx_counts (type, token0, token1, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
            @_txs_stmt);
        PREPARE stmt FROM @_txs_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF _ucnt > 0 THEN
            SET @_usr_stmt = CONCAT("REPLACE INTO _user_counts (type, token0, token1, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
                @_usr_stmt);
            PREPARE stmt FROM @_usr_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        IF _nproc >= 1000 THEN
            -- flush from the temporary to the real
            REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
            REPLACE INTO user_counts SELECT * FROM _user_counts;
	    -- commit happens here
            -- TRUNCATE _tx_counts;
            -- TRUNCATE _user_counts;
	    DELETE FROM _tx_counts;
	    DELETE FROM _user_counts;

            CALL tx_counts_trim();
            SET _nproc = 0;
        ELSE
            SET _nproc = _nproc + 1;
        END IF;

    END LOOP iter;
    CLOSE cur;

    -- flush from the temporary to the real
    REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
    REPLACE INTO user_counts SELECT * FROM _user_counts;

    DROP TEMPORARY TABLE _tx_counts;
    DROP TEMPORARY TABLE _user_counts;
    -- SET @_txs_stmt = "", @_usr_stmt = "";

END ///
DELIMITER ;

DROP TABLE IF EXISTS ooo;
CREATE TABLE ooo (
  i1 int,
  i2 int,
  k1 varchar(32),
  primary key (k1)
);

-- assumes txs.txhistory has all the entries, so that union with liquidationtxhistory is unnecessary
-- "TEXT" is not indexible, used the following schema
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
-- uses two indexes
  - for type & token oriented: INDEX (type, token0, token1, user, id DESC),
  - user oriented:             INDEX (user, type, token0, token1, id DESC),

-- tx and user counts and their time series are maintained in separate tables
 -- time_unit = 0 contains total counts
 -- time_unit = 1, 60, 3600, 86400 for second, minute, hour and day time series
    respectively
 -- updating this is very slow: about 10 ms fo a single entry, which requires
   -- either background update
   -- batch update

DROP TABLE IF EXISTS tx_counts;
CREATE TABLE tx_counts (
  type VARCHAR(16),
  token0 VARCHAR(44),
  token1 VARCHAR(44),
  user VARCHAR(44),
  amount INT,
  count INT,
  block_number INT,
  block_time DATETIME,
  time_unit INT,
  time_slot INT,
  updated_time TIMESTAMP,

  PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC, user ASC),
  INDEX (time_unit ASC, type ASC, token0 ASC, token1 ASC, user ASC, time_slot DESC),
  INDEX (time_unit ASC, user ASC, type ASC, token0 ASC, token1 ASC, time_slot DESC),
  -- to get the latest block number
  INDEX (block_number DESC),
  -- for trimming
  INDEX (time_unit ASC, block_time DESC)
);

DROP TABLE IF EXISTS user_counts;
CREATE TABLE user_counts (
  type VARCHAR(16),
  token0 VARCHAR(44),
  token1 VARCHAR(44),
  count INT,
  block_number INT,
  block_time DATETIME,
  time_unit INT,
  time_slot INT,
  updated_time TIMESTAMP,

  PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC),
  INDEX (time_unit ASC, type ASC, token0 ASC, token1 ASC, time_slot DESC),
  -- to get the latest block number
  INDEX (block_number DESC),
  -- for trimming
  INDEX (time_unit ASC, block_time DESC)
);

-- function to update tx_counts & user_counts
-- e.g.: call txs.tx_counts_proc_block(0, 0)
DROP PROCEDURE IF EXISTS tx_counts_proc_blocks;
DELIMITER ///
CREATE PROCEDURE tx_counts_proc_blocks(_start INT, _end INT)
BEGIN
    DECLARE _done INT DEFAULT 0;
    DECLARE _type, __type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amt, _cnt INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE _now TIMESTAMP DEFAULT NOW();
    DECLARE _ts1, _ts60, _ts3600, _ts86400 INT;
    DECLARE __token0, __token1, __user VARCHAR(44);
    DECLARE __amt, __cnt, __tunit, __tslot INT;
    DECLARE _i, _j INT;
    DECLARE _ucnt, _nproc INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT type, token0, token1, user, block_number, block_timestamp
            FROM txhistory WHERE
                _start <= block_number AND (_end <= 0 OR block_number <= _end)
            ORDER BY block_number ASC;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _done = 1;

    -- need to specify indexes including primary key
    CREATE TEMPORARY TABLE _tx_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC, user ASC))
        SELECT * FROM tx_counts LIMIT 0;
    CREATE TEMPORARY TABLE _user_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1))
        SELECT * FROM user_counts LIMIT 0;

    OPEN cur;
    iter: LOOP
        SET _done = 0;
        FETCH cur INTO _type, _token0, _token1, _user, _bnum, _btime;
        IF _done = 1 THEN
            LEAVE iter;
        END IF;
        IF ISNULL(_btime) THEN
            ITERATE iter;
        END IF;

        SET @_txs_stmt = "", @_usr_stmt = "";

        -- TODO: ts86400 needs to use TIMEZONE, it's in UTC for now
        SET _ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
            _ts60 = _ts1 DIV 60 * 60,
            _ts3600 = _ts1 DIV 3600 * 3600,
            _ts86400 = _ts1 DIV 86400 * 86400;
    
        SET _i = 0, _ucnt = 0;
        WHILE _i < 6 DO
            IF _i = 0 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 1 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 2 THEN
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = ' ';
            ELSEIF _i = 3 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = _user;
            ELSEIF _i = 4 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = _user;
            ELSE
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = _user;
            END IF;
            SET _i = _i + 1;
    
            SET _j = 0;
            WHILE (_i < 3 AND _j < 5) OR (3 <= _i AND _j = 0) DO
                IF _j = 0 THEN
                    SET __tunit = 0, __tslot = 0;
                ELSEIF _j = 1 THEN
                    SET __tunit = 1, __tslot = _ts1;
                ELSEIF _j = 2 THEN
                    SET __tunit = 60, __tslot = _ts60;
                ELSEIF _j = 3 THEN
                    SET __tunit = 3600, __tslot = _ts3600;
                ELSE
                    SET __tunit = 86400, __tslot = _ts86400;
                END IF;
                SET _j = _j + 1;
    
                -- tx_counts table
                -- from temporary table, then if not present, from tx_counts 
                SET __cnt = 0, __amt = 0;
                SELECT count, count INTO __cnt, __amt FROM _tx_counts WHERE
                    type = __type AND token0 = __token0 AND token1 = __token1
	       	    AND user = __user AND time_unit = __tunit
		    AND time_slot = __tslot;
                IF __cnt = 0 THEN
                    SELECT count, count INTO __cnt, __amt FROM tx_counts WHERE
                        type = __type AND token0 = __token0 AND token1 = __token1
			AND user = __user AND time_unit = __tunit
			AND time_slot = __tslot;
                END IF;
    
                IF (NOT _j = 1) OR (NOT _i = 1) THEN
                    SET @_txs_stmt = CONCAT(@_txs_stmt, ",");
                END IF;
                SET @_txs_stmt = CONCAT(@_txs_stmt, "('", __type, "','",
                        __token0, "','", __token1, "','", __user, "',",
		       	__amt + _amt, ",", __cnt + _cnt, ",", _bnum, ",'",
		       	_btime, "',", __tunit, ",", __tslot, ",'", _now, "')");
    
                -- user_counts table
                IF (NOT __user = ' ') AND __cnt = 0 THEN
                    -- from temporary table, then if not present, from tx_counts 
                    SET __cnt = 0;
                    SELECT count INTO __cnt FROM _user_counts WHERE
                        type = __type AND token0 = __token0 AND token1 = __token1
			AND time_unit = __tunit AND time_slot = __tslot;
                    IF __cnt = 0 THEN
                        SELECT count INTO __cnt FROM user_counts WHERE
                            type = __type AND token0 = __token0 AND token1 = __token1
			    AND time_unit = __tunit AND time_slot = __tslot;
                    END IF;
    
                    IF NOT _ucnt = 0 THEN
                        SET @_usr_stmt = CONCAT(@_usr_stmt, ",");
                    END IF;
                    SET @_usr_stmt = CONCAT(@_usr_stmt, "('", __type, "','",
                            __token0, "','", __token1, "',", __cnt + 1, ",",
			    _bnum, ",'", _btime, "',", __tunit, ",",
			    __tslot, ",'", _now, "')");
                    SET _ucnt = _ucnt + 1;
                END IF;
            END WHILE;
        END WHILE;

        SET @_txs_stmt = CONCAT("REPLACE INTO _tx_counts (type, token0, token1, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
            @_txs_stmt);
        PREPARE stmt FROM @_txs_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF _ucnt > 0 THEN
            SET @_usr_stmt = CONCAT("REPLACE INTO _user_counts (type, token0, token1, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
                @_usr_stmt);
            PREPARE stmt FROM @_usr_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        IF _nproc >= 1000 THEN
            -- flush from the temporary to the real
            REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
            REPLACE INTO user_counts SELECT * FROM _user_counts;
	    -- commit happens here
            -- TRUNCATE _tx_counts;
            -- TRUNCATE _user_counts;
	    DELETE FROM _tx_counts;
	    DELETE FROM _user_counts;

            CALL tx_counts_trim();
            SET _nproc = 0;
        ELSE
            SET _nproc = _nproc + 1;
        END IF;

    END LOOP iter;
    CLOSE cur;

    -- flush from the temporary to the real
    REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
    REPLACE INTO user_counts SELECT * FROM _user_counts;

    DROP TEMPORARY TABLE _tx_counts;
    DROP TEMPORARY TABLE _user_counts;
    -- SET @_txs_stmt = "", @_usr_stmt = "";

END ///
DELIMITER ;

DROP TABLE IF EXISTS tx_counts_lock;
CREATE TABLE tx_counts_lock (
    id INT,
    block_number INT,
    name INT,		-- always 1
    PRIMARY KEY (name)
);

DROP PROCEDURE IF EXISTS tx_counts_proc;
DELIMITER ///
CREATE PROCEDURE tx_counts_proc()
BEGIN
    DECLARE _id, _bnum INT DEFAULT -1;
    DECLARE _max INT DEFAULT 10000;
    DECLARE _done INT DEFAULT 0;
    
    WHILE _done = 0 DO
        START TRANSACTION;
        -- lock it
        SELECT id, block_number INTO _id, _bnum FROM tx_counts_lock
            WHERE name = 1 FOR UPDATE;
        CALL tx_counts_proc_txs(_id, _max, @_last_id, @_last_block);
SELECT "done", _id, _max, "->", @_last_id, @_last_block;
	IF @_last_id = -1 THEN
	    SET _done = 1;
	    ROLLBACK;
	ELSE
	    REPLACE INTO tx_counts_lock
                SET id = @_last_id, block_number = @_last_block, name = 1;
	    COMMIT;
	END IF;
	SET @_last_id = NULL, @_last_block = NULL;
    END WHILE;
END ///
DELIMITER ;


DROP PROCEDURE IF EXISTS tx_counts_proc_txs;
DELIMITER ///
CREATE PROCEDURE tx_counts_proc_txs(IN _start_id INT, IN _max INT, OUT _last_id INT, OUT _last_block INT)
BEGIN
    -- lock it
    DECLARE _id INT DEFAULT -1;
    DECLARE _done INT DEFAULT 0;
    DECLARE _type, __type VARCHAR(16) DEFAULT NULL;
    DECLARE _token0, _token1, _user VARCHAR(44);
    DECLARE _amt, _cnt INT DEFAULT 1;
    DECLARE _bnum INT;
    DECLARE _btime TIMESTAMP;
    DECLARE _now TIMESTAMP DEFAULT NOW();
    DECLARE _ts1, _ts60, _ts3600, _ts86400 INT;
    DECLARE __token0, __token1, __user VARCHAR(44);
    DECLARE __amt, __cnt, __tunit, __tslot INT;
    DECLARE _i, _j INT;
    DECLARE _ucnt, _nproc INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT id, type, token0, token1, user, block_number, block_timestamp
            FROM txhistory WHERE id > _start_id ORDER BY id ASC;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _done = 1;

    -- need to specify indexes including primary key
    CREATE TEMPORARY TABLE _tx_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1 ASC, user ASC))
        SELECT * FROM tx_counts LIMIT 0;
    CREATE TEMPORARY TABLE _user_counts
        (PRIMARY KEY (time_unit ASC, time_slot DESC, type ASC, token0 ASC, token1))
        SELECT * FROM user_counts LIMIT 0;

    OPEN cur;
    iter: LOOP
        SET _done = 0;
        FETCH cur INTO _id, _type, _token0, _token1, _user, _bnum, _btime;
        IF _done = 1 THEN
            LEAVE iter;
        END IF;
        IF ISNULL(_btime) THEN
            ITERATE iter;
        END IF;

        SET @_txs_stmt = "", @_usr_stmt = "";

        -- TODO: ts86400 needs to use TIMEZONE, it's in UTC for now
        SET _ts1 = FLOOR(UNIX_TIMESTAMP(_btime)),
            _ts60 = _ts1 DIV 60 * 60,
            _ts3600 = _ts1 DIV 3600 * 3600,
            _ts86400 = _ts1 DIV 86400 * 86400;
    
        SET _i = 0, _ucnt = 0;
        WHILE _i < 6 DO
            IF _i = 0 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 1 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = ' ';
            ELSEIF _i = 2 THEN
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = ' ';
            ELSEIF _i = 3 THEN
                SET __type = ' ', __token0 = ' ', __token1 = ' ', __user = _user;
            ELSEIF _i = 4 THEN
                SET __type = _type, __token0 = ' ', __token1 = ' ', __user = _user;
            ELSE
                SET __type = _type, __token0 = _token0, __token1 = _token1, __user = _user;
            END IF;
            SET _i = _i + 1;
    
            SET _j = 0;
            WHILE (_i < 3 AND _j < 5) OR (3 <= _i AND _j = 0) DO
                IF _j = 0 THEN
                    SET __tunit = 0, __tslot = 0;
                ELSEIF _j = 1 THEN
                    SET __tunit = 1, __tslot = _ts1;
                ELSEIF _j = 2 THEN
                    SET __tunit = 60, __tslot = _ts60;
                ELSEIF _j = 3 THEN
                    SET __tunit = 3600, __tslot = _ts3600;
                ELSE
                    SET __tunit = 86400, __tslot = _ts86400;
                END IF;
                SET _j = _j + 1;
    
                -- tx_counts table
                -- from temporary table, then if not present, from tx_counts 
                SET __cnt = 0, __amt = 0;
                SELECT count, count INTO __cnt, __amt FROM _tx_counts WHERE
                    type = __type AND token0 = __token0 AND token1 = __token1
	       	    AND user = __user AND time_unit = __tunit
		    AND time_slot = __tslot;
                IF __cnt = 0 THEN
                    SELECT count, count INTO __cnt, __amt FROM tx_counts WHERE
                        type = __type AND token0 = __token0 AND token1 = __token1
			AND user = __user AND time_unit = __tunit
			AND time_slot = __tslot;
                END IF;
    
                IF (NOT _j = 1) OR (NOT _i = 1) THEN
                    SET @_txs_stmt = CONCAT(@_txs_stmt, ",");
                END IF;
                SET @_txs_stmt = CONCAT(@_txs_stmt, "('", __type, "','",
                        __token0, "','", __token1, "','", __user, "',",
		       	__amt + _amt, ",", __cnt + _cnt, ",", _bnum, ",'",
		       	_btime, "',", __tunit, ",", __tslot, ",'", _now, "')");
    
                -- user_counts table
                IF (NOT __user = ' ') AND __cnt = 0 THEN
                    -- from temporary table, then if not present, from tx_counts 
                    SET __cnt = 0;
                    SELECT count INTO __cnt FROM _user_counts WHERE
                        type = __type AND token0 = __token0 AND token1 = __token1
			AND time_unit = __tunit AND time_slot = __tslot;
                    IF __cnt = 0 THEN
                        SELECT count INTO __cnt FROM user_counts WHERE
                            type = __type AND token0 = __token0 AND token1 = __token1
			    AND time_unit = __tunit AND time_slot = __tslot;
                    END IF;
    
                    IF NOT _ucnt = 0 THEN
                        SET @_usr_stmt = CONCAT(@_usr_stmt, ",");
                    END IF;
                    SET @_usr_stmt = CONCAT(@_usr_stmt, "('", __type, "','",
                            __token0, "','", __token1, "',", __cnt + 1, ",",
			    _bnum, ",'", _btime, "',", __tunit, ",",
			    __tslot, ",'", _now, "')");
                    SET _ucnt = _ucnt + 1;
                END IF;
            END WHILE;
        END WHILE;

        SET @_txs_stmt = CONCAT("REPLACE INTO _tx_counts (type, token0, token1, user, amount, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
            @_txs_stmt);
        PREPARE stmt FROM @_txs_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF _ucnt > 0 THEN
            SET @_usr_stmt = CONCAT("REPLACE INTO _user_counts (type, token0, token1, count, block_number, block_time, time_unit, time_slot, updated_time) VALUES ",
                @_usr_stmt);
            PREPARE stmt FROM @_usr_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        SET _nproc = _nproc + 1;
        IF _nproc >= _max THEN
            LEAVE iter;
	END IF;

    END LOOP iter;
    CLOSE cur;

    -- flush from the temporary to the real
    IF _nproc > 0 THEN
        REPLACE INTO tx_counts   SELECT * FROM _tx_counts;
        REPLACE INTO user_counts SELECT * FROM _user_counts;

        DROP TEMPORARY TABLE _tx_counts;
        DROP TEMPORARY TABLE _user_counts;
        SET _last_id = _id, _last_block = _bnum;
    ELSE
        DROP TEMPORARY TABLE _tx_counts;
        DROP TEMPORARY TABLE _user_counts;
        SET _last_id = -1, _last_block = -1;
    END IF;

    -- SET @_txs_stmt = "", @_usr_stmt = "";
END ///
DELIMITER ;

DROP PROCEDURE IF EXISTS tx_counts_trim;
DELIMITER ///
CREATE PROCEDURE tx_counts_trim()
BEGIN
    DECLARE t_last INT;
    DECLARE duration_seconds INT DEFAULT 3600;
    DECLARE duration_minutes INT DEFAULT 86400;
    DECLARE duration_hours   INT DEFAULT 2678400;
    SELECT FLOOR(UNIX_TIMESTAMP(block_time)) INTO t_last
        FROM tx_counts ORDER BY block_number DESC LIMIT 1,1;
    DELETE FROM tx_counts WHERE time_unit = 1 AND
        block_time < FROM_UNIXTIME(t_last - duration_seconds);
    DELETE FROM tx_counts WHERE time_unit = 60 AND
        block_time < FROM_UNIXTIME(t_last - duration_minutes);
    DELETE FROM tx_counts WHERE time_unit = 3600 AND
        block_time < FROM_UNIXTIME(t_last - duration_hours);
END ///
DELIMITER ;

-- ** queries
-- counts queries
node sqlizer.js mquery <threads> <counts> <sql-statement> <0|1>
-- total txs
  - -> 0.5 ms
  SELECT * FROM txs.tx_counts WHERE type = ' ' and user = ' ' and time_unit = 0;
  - by type -> 0.5 ms
  SELECT * FROM txs.tx_counts WHERE type = 'SWAP' and token0 = ' '
    and user = ' ' and time_unit = 0;
  - by type & token -> 0.8 ms, multiple instances, requires SUM
  SELECT sum(count) FROM txs.tx_counts WHERE type = 'SWAP' and
   (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
    token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') and
    user = ' ' and time_unit = 0;
  - by user -> 0.4 ms
  SELECT * FROM txs.tx_counts WHERE type = ' ' and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' and time_unit = 0;
  - by type, token & user -> 0.6 ms
  SELECT sum(count) FROM txs.tx_counts WHERE type = 'SWAP' and
   (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
    token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' and time_unit = 0;
-- txs time series
  - total counts per second -> 2ms, 789 rows
  SELECT * FROM txs.tx_counts WHERE type = ' ' and user = ' ' and time_unit = 1;
  - total counts per minute -> 4ms, 1250 rows
  SELECT * FROM txs.tx_counts WHERE type = ' ' and user = ' ' and time_unit = 60;
  - total counts per hour -> 4ms, 751 rows
  SELECT * FROM txs.tx_counts WHERE type = ' ' and user = ' ' and time_unit = 3600;
  - total counts per day -> 1ms, 144 rows
  SELECT * FROM txs.tx_counts WHERE type = ' ' and user = ' ' and time_unit = 86400;
  - counts with type per second, minute, hour, day -> 1.5 ms, 3.3 ms, 2.4 ms, 0.9 ms
  SELECT sum(count) FROM txs.tx_counts WHERE type = 'SWAP' and
   (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
    token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') and
    user = ' ' and time_unit = 1;
  ...
-- total users, no time series
  - total users -> 0.4 ms
  SELECT * FROM user_counts WHERE type = ' ' and time_unit = 0;
  - with type
  SELECT * FROM user_counts WHERE type = 'SWAP' and token0 = ' ' and time_unit = 0;
  - with type and token, multiple instances, requires SUM
  SELECT SUM(count) FROM user_counts WHERE type = 'SWAP' and token0 = ' ' and time_unit = 0;
  SELECT SUM(count) FROM user_counts WHERE type = 'SWAP' and
    token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' and time_unit = 0;

-- non-count queries
-- with type
  -> 1 entries 0.4ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' limit 1,1;
  -> 1000 entries 3ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' limit 1000,1;
  -> 308039 entries 558 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' limit 308039,1;
-- with type and token
  -> 1 entries 0.5ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
     token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') LIMIT 1,1;
  -> 1000 entries 14 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
     token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') LIMIT 1000,1;
  -> 179853 entries 616 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
     token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') LIMIT 179852,1;
-- with user
  -> 1 entries 0.5ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' LIMIT 1,1;
  -> 1000 entries 3 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' LIMIT 1000,1;
  -> 90801 entries 186 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' LIMIT 90800,1;
-- with type, token & user
  -> 1 entries 0.5ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
     token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' LIMIT 1,1;
  -> 1000 entries 4 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
     token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' LIMIT 1000,1;
  -> 90801 entries 220 ms
  SELECT * FROM txs.txhistory WHERE type = 'SWAP' and
    (token0 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1' or
     token1 = '0x8E81fCc2d4A3bAa0eE9044E0D7E36F59C9BbA9c1') and
    user = '0x543666750d286f22aD9ce91Af6Ce6EB99a1F9a74' LIMIT 90800,1;

-- on tg04
$ bobthe.sh shell mariothe
$ mysql -u admin txs
> 

--
1) 23 txs in txhistory have block_time = NULL
2) alter table & indexes. VARCHAR(255)로는 인덱스를 만들수가 없음. 
ALTER TABLE txs.txhistory
  MODIFY address VARCHAR(42) NOT NULL,
  MODIFY type VARCHAR(32) NOT NULL,
  MODIFY tx_hash VARCHAR(66) NOT NULL,
  MODIFY func_sig VARCHAR(32) NOT NULL,
  MODIFY user VARCHAR(42) NOT NULL,
  MODIFY token0 VARCHAR(42) NOT NULL,
  MODIFY token1 VARCHAR(42) NOT NULL;
CREATE INDEX ix_type ON txs.txhistory (type, token0, token1, user, id DESC);
CREATE INDEX ix_user ON txs.txhistory (user, type, token0, token1, id DESC);
3) tx_counts & user_counts update
  -- batch processing for now every 15 seconds



-- EOF
