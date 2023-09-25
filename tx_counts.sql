-- tx_counts related

-- CALL tx_counts_proc()

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

-- EOF
