const Redis = require('ioredis');
const HttpClient = require('./httpClient');
const util = require('./util');
// const { Pool } = require('pg');

var ShareProcessor = module.exports = function ShareProcessor(config, logger){
    var confirmationTime = config.confirmationTime * 1000;
    var rewardPercent = 1 - config.withholdPercent;

    var _this = this;
    const baseName = config.redis.baseName ? config.redis.baseName : 'alephium';
    this.redisClient = new Redis(config.redis.port, config.redis.host, {db: config.redis.db});
    this.httpClient = new HttpClient(config.daemon.host, config.daemon.port, config.daemon.apiKey);

    // function createTables(db){
    //     var tables =
    //         `CREATE TABLE IF NOT EXISTS "shares" (
    //             "from_group" SMALLINT NOT NULL,
    //             "to_group" SMALLINT NOT NULL,
    //             "pool_diff" NUMERIC(13, 8) NOT NULL,
    //             "share_diff" NUMERIC(13, 8) NOT NULL,
    //             "worker" VARCHAR(64) NOT NULL,
    //             "found_block" BOOLEAN NOT NULL,
    //             "created_date" TIMESTAMP,
    //             "modified_date" TIMESTAMP,
    //             "id" SERIAL PRIMARY KEY
    //         );
    //         CREATE TABLE IF NOT EXISTS "blocks" (
    //             "share_id" INTEGER NOT NULL,
    //             "from_group" SMALLINT NOT NULL,
    //             "to_group" SMALLINT NOT NULL,
    //             "block_hash" CHAR(64) NOT NULL,
    //             "worker" VARCHAR(64) NOT NULL,
    //             "created_date" TIMESTAMP,
    //             "modified_date" TIMESTAMP,
    //             "id" SERIAL PRIMARY KEY
    //         );`;
    //     db.query(tables, function(error, _){
    //         if (error) {
    //             logger.error('Create table error: ' + error);
    //             process.exit(1);
    //         }
    //     });
    // }

    // function persistShare(db, share){
    //     db.query(
    //         'INSERT INTO shares(from_group, to_group, pool_diff, share_diff, worker, found_block, created_date, modified_date) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id',
    //         [share.job.fromGroup, share.job.toGroup, share.difficulty, share.shareDiff, share.workerAddress, share.foundBlock, new Date(), new Date()],
    //         function(error, result){
    //             if (error) {
    //                 logger.error('Persist share error: ' + error);
    //                 return;
    //             }
    //
    //             if (share.foundBlock){
    //                 var shareId = result.rows[0].id;
    //                 db.query(
    //                     'INSERT INTO blocks(share_id, from_group, to_group, block_hash, worker, created_date, modified_date) VALUES($1, $2, $3, $4, $5, $6, $7)',
    //                     [shareId, share.job.fromGroup, share.job.toGroup, share.blockHash, share.workerAddress, new Date(), new Date()],
    //
    //                     function(error, _){
    //                         if (error) logger.error('Persist block error: ' + error);
    //                     }
    //                 );
    //             }
    //         }
    //     );
    // }

    // if (config.persistence && config.persistence.enabled) {
    //     _this.db = new Pool(config.persistence);
    //     createTables(_this.db);
    //     _this.handleShare = function(share){
    //         persistShare(_this.db, share);
    //         _this._handleShare(share);
    //     }
    // }
    // else {
    _this.handleShare = share => _this._handleShare(share);

    this.currentRoundKey = function(fromGroup, toGroup){
        return baseName + ':' + fromGroup + ':' + toGroup + ':shares:currentRound';
    }

    this.roundKey = function(fromGroup, toGroup, blockHash){
        return baseName + ':' + fromGroup + ':' + toGroup + ':shares:' + blockHash;
    }

    var pendingBlocksKey = baseName + ':' + 'pendingBlocks';
    var foundBlocksKey = baseName + ':' + 'foundBlocks';
    var hashrateKey = baseName + ':' + 'hashrate';
    var balancesKey = baseName + ':' + 'balances';

    this._handleShare = function(share){
        var redisTx = _this.redisClient.multi();
        var currentMs = Date.now();
        var fromGroup = share.job.fromGroup;
        var toGroup = share.job.toGroup;
        var currentRound = _this.currentRoundKey(fromGroup, toGroup);
        redisTx.hincrbyfloat(currentRound, share.workerAddress, share.difficulty);

        const shareValue = share.difficulty;
        // const shareValue = share.difficulty * 16 * Math.pow(2, config.diff1TargetNumZero);
        redisTx.hincrbyfloat(baseName + ':' + 'stats', 'roundShares', shareValue);

        var currentTs = Math.floor(currentMs / 1000);
        var worker = share.worker.replace(share.workerAddress, '').replace('.', '');
        if (!worker) {
            worker = 'default';
        }
        redisTx.zadd(hashrateKey, currentTs, [fromGroup, toGroup, share.workerAddress, worker, share.difficulty, currentMs].join(':'));

        redisTx.zadd(hashrateKey + ':' + share.workerAddress, currentTs, [fromGroup, toGroup, worker, share.difficulty, currentMs].join(':'));

        redisTx.exec(function(error, _){
            if (error) logger.error('Handle share failed, error: ' + error);

            if (share.foundBlock){
            // if (true) {
                PutBlock(share);
                // var blockHash = share.blockHash;
                // var newKey = _this.roundKey(fromGroup, toGroup, blockHash);
                // var blockWithTs = blockHash + ':' + currentMs.toString();
                //
                // redisTx.rename(currentRound, newKey);
                // redisTx.sadd(pendingBlocksKey, blockWithTs);
                // redisTx.hset(foundBlocksKey, blockHash, share.workerAddress);
                //
                // redisTx.hset(baseName + ':' + 'stats', 'lastBlockFound', currentTs);

            }
        });
    }

    function PutBlock(share) {
        _this.redisClient.hgetall(baseName + ':' + 'stats', function(error, result) {
            if (error) {
                logger.error('Get stats failed, error: ' + error);
            }

            const roundShares = result.roundShares | 0;
            const difficulty = result.difficulty | 0;
            // console.log('roundShares', roundShares);
            // console.log('difficulty', difficulty);

            var redisTx = _this.redisClient.multi();
            var currentMs = Date.now();
            var currentTs = Math.floor(currentMs / 1000);
            var blockHash = share.blockHash;
            var newKey = _this.roundKey(share.job.fromGroup, share.job.toGroup, blockHash);
            var blockWithTs = blockHash + ':' + currentMs.toString();

            // REMOVE LATER
            var currentRound = _this.currentRoundKey(share.job.fromGroup, share.job.toGroup);
            redisTx.rename(currentRound, newKey);
            redisTx.sadd(pendingBlocksKey, blockWithTs);
            redisTx.hset(foundBlocksKey, blockHash, share.workerAddress);
            // REMOVE LATER END

            redisTx.hset(baseName + ':' + 'stats', 'lastBlockFound', currentTs);
            redisTx.hdel(baseName + ':' + 'stats', 'roundShares');

            // GetBlockHeight('00000000000b7ab9f0f58e544c1cb9af564fe6c1bff29613e04c6c10598330d9', function (height) {
            //     console.log('HEIGHT!!!', height)

            GetBlockHeight(share.blockHash, function (height) {
                redisTx.zadd(baseName + ':' + 'blocks:candidates', currentTs, [
                    share.job.fromGroup, share.job.toGroup,
                    height, share.blockHash, share.workerAddress, roundShares, difficulty].join(':'));

                redisTx.exec(function(error, _){
                    if (error) logger.error('Put block failed, error: ' + error);
                });
            })
        })
    }

    function GetBlockHeight(blockHash, callback) {
        _this.httpClient.getBlock(blockHash, function(result){
            if (result.error){
                logger.error('Get block error: ' + result.error + ', hash: ' + blockHash);
                callback();
                return;
            }
            console.log(result)
            result.height ? callback(result.height) : callback();
        });
    }


    function handleBlock(block, callback){
        var transactions = block.transactions;
        var rewardTx = transactions[transactions.length - 1];
        var rewardOutput = rewardTx.unsigned.fixedOutputs[0];
        var blockData = {
            hash: block.hash,
            fromGroup: block.chainFrom,
            toGroup: block.chainTo,
            height: block.height,
            rewardAmount: rewardOutput.attoAlphAmount // string
        };
        callback(blockData);
    }

    // remove block shares and remove blockHash from pendingBlocks
    function removeBlockAndShares(fromGroup, toGroup, blockHash, blockHashWithTs){
        _this.redisClient
            .multi()
            .del(_this.roundKey(fromGroup, toGroup, blockHash))
            .srem(pendingBlocksKey, blockHashWithTs)
            .hdel(foundBlocksKey, blockHash)
            .exec(function(error, _){
                if (error) logger.error('Remove block shares failed, error: ' + error + ', blockHash: ' + blockHash);
            })
    }
    function removeBlockAndShares2(fromGroup, toGroup, blockHash, blockKey){
        _this.redisClient
            .multi()
            .del(_this.roundKey(fromGroup, toGroup, blockHash))
            .zrem(baseName + ':blocks:candidates', blockKey)

            // .srem(pendingBlocksKey, blockHashWithTs)
            // .hdel(foundBlocksKey, blockHash)
        // r.client.ZRem(ctx, r.formatKey("blocks", "candidates"), b.CandidateKey)
            .exec(function(error, _){
                if (error) logger.error('Remove block shares failed, error: ' + error + ', blockHash: ' + blockHash);
            })
    }

    this.getPendingBlocks = function(results, callback){
        var blocksNeedToReward = [];
        util.executeForEach(results, function(blockHashWithTs, callback){
            var array = blockHashWithTs.split(':');
            var blockHash = array[0];
            var blockSubmitTs = parseInt(array[1]);
            var now = Date.now();

            if (now < (blockSubmitTs + confirmationTime)){
                // the block reward might be locked, skip and
                // try to reward in the next loop
                callback();
                return;
            }

            _this.httpClient.blockInMainChain(blockHash, function(result){
                if (result.error){
                    logger.error('Check block in main chain error: ' + result.error);
                    callback();
                    return;
                }

                if (!result){
                    logger.error('Block is not in mainchain, remove block and shares, hash: ' + blockHash);
                    var [fromGroup, toGroup] = util.blockChainIndex(Buffer.from(blockHash, 'hex'));
                    removeBlockAndShares(fromGroup, toGroup, blockHash, blockHashWithTs);
                    callback();
                    return;
                }

                _this.httpClient.getBlock(blockHash, function(result){
                    if (result.error){
                        logger.error('Get block error: ' + result.error + ', hash: ' + blockHash);
                        callback();
                        return;
                    }

                    handleBlock(result, function(blockData){
                        var block = {
                            pendingBlockValue: blockHashWithTs,
                            data: blockData
                        };
                        blocksNeedToReward.push(block);
                        callback();
                    });
                });
            });
        }, _ => callback(blocksNeedToReward));
    }

    // this.allocateRewards = function(blocks, callback){
    //     var workerRewards = {};
    //     var redisTx = _this.redisClient.multi();
    //     util.executeForEach(blocks, function(block, callback){
    //         allocateRewardForBlock(block, redisTx, workerRewards, callback);
    //     }, function(_){
    //         for (var worker in workerRewards){
    //             let workerRewardSum = 0;
    //             for (let data in worker) {
    //                 redisTx.zadd(baseName + ':rewards:' + worker, data.timestamp, [
    //                     data.reward, data.share, data.hash, data.height].join(':'));
    //
    //                 workerRewardSum += data.reward;
    //             }
    //             redisTx.hincrbyfloat(baseName + ':miners:' + worker, 'balance', workerRewardSum);
    //
    //             redisTx.hincrbyfloat(balancesKey, worker, workerRewardSum);
    //         }
    //         redisTx.exec(function(error, _){
    //             if (error) {
    //                 logger.error('Allocate rewards failed, error: ' + error);
    //                 callback(error);
    //                 return;
    //             }
    //             logger.debug('Rewards: ' + JSON.stringify(workerRewards));
    //             callback(null);
    //         });
    //     });
    // }

    this.allocateRewards = function(blocks, callback){
        var workerRewards = {};
        var redisTx = _this.redisClient.multi();
        util.executeForEach(blocks, function(block, callback){
            allocateRewardForBlock(block, redisTx, workerRewards, callback);
        }, function(_){
            for (var worker in workerRewards){
                let workerRewardSum = 0;
                for (let data in worker) {
                    redisTx.zadd(baseName + ':rewards:' + worker, data.timestamp, [
                        data.reward, data.share, data.hash, data.height].join(':'));

                    workerRewardSum += data.reward;
                }
                redisTx.hincrbyfloat(baseName + ':miners:' + worker, 'balance', workerRewardSum);

                redisTx.hincrbyfloat(balancesKey, worker, workerRewardSum);
            }
            redisTx.exec(function(error, _){
                if (error) {
                    logger.error('Allocate rewards failed, error: ' + error);
                    callback(error);
                    return;
                }
                logger.debug('Rewards: ' + JSON.stringify(workerRewards));
                callback(null);
            });
        });
    }

    function allocateRewardForBlock(block, redisTx, workerRewards, callback){
        // var blockData = block.data;
        var round = _this.roundKey(block.fromGroup, block.toGroup, block.hash);
        _this.redisClient.hgetall(round, function(error, shares){
            if (error) {
                logger.error('Get shares failed, error: ' + error + ', round: ' + round);
                callback();
                return;
            }

            var totalReward = Math.floor(parseInt(block.rewardAmount) * rewardPercent);
            logger.info('Reward miners for block: ' + block.hash + ', total reward: ' + totalReward);
            logger.debug('Block hash: ' + block.hash + ', shares: ' + JSON.stringify(shares));
            _this.allocateReward(totalReward, workerRewards, shares, block);

            redisTx.del(round);
            redisTx.zrem(baseName + ':blocks:candidates', block.key)
            redisTx.zadd(baseName + ':' + 'blocks:confirmed', block.timestamp, [
                block.fromGroup, block.toGroup,
                block.height, block.hash, block.finder, block.roundShares, block.difficulty].join(':'));

            // redisTx.srem(pendingBlocksKey, block.pendingBlockValue);
            logger.info('Remove shares for block: ' + block.hash);
            callback();
        });
    }

    this.allocateReward = function(totalReward, workerRewards, shares, block){
        var totalShare = Object.keys(shares).reduce(function(acc, worker){
            return acc + parseFloat(shares[worker]);
        }, 0);

        for (var worker in shares){
            var percent = parseFloat(shares[worker]) / totalShare;
            var workerReward = util.toALPH(totalReward * percent);
            const rewardObj = {
                height:     block.height,
                reward:     workerReward,
                hash:       block.hash,
                timestamp:  block.timestamp,
                share:      percent
            };
            // console.log(rewardObj)
            if (workerRewards[worker]){
                workerRewards[worker].push(rewardObj)
            } else {
                workerRewards[worker] = [rewardObj]
            }
        }
    }

    // function scanBlocks(){
    //     _this.redisClient.smembers(pendingBlocksKey, function(err, results){
    //         if (err){
    //             logger.error('Get pending blocks failed, error: ' + err);
    //             return;
    //         }
    //         _this.getPendingBlocks(results, function(blocks){
    //             _this.allocateRewards(blocks, _ => {});
    //         });
    //     })
    //
    //     _this.redisClient.zrangebyscore(cfg.baseName + ':' + 'candidates', '-inf', '+inf', function(err, result){
    //         if (err){
    //             logger.error('Get candidates failed, error: ' + err);
    //             return;
    //         }
    //         console.log(result)
    //     })
    // }

    function scanBlocks(){
        _this.redisClient.zrangebyscore(baseName + ':' + 'blocks:candidates', '-inf', '+inf', 'WITHSCORES', function(err, result){
            if (err){
                logger.error('Get candidates failed, error: ' + err);
                return;
            }

            let candidates = [];

            if (result.length) {
                for (let i = 0; i + 1 < result.length; i = i + 2) {
                    // 3                                                                        0   FromGroup
                    // :0                                                                       1   ToGroup
                    // :763094 (maybe no data)                                                  2   Height
                    // :00000000085c670186d65ab28147369f87b211eb1b6ab9d6d93a35232b9e2c6c        3   BlockHash
                    // :199TYpbRaufnnUtjmN2ninQgk1YaYComiryb1SDoZk74K                           4   Finder
                    // :4                                                                       5   roundShares
                    // :0                                                                       6   difficulty
                    // score: '44566155'                                                            Timestamp
                    const blockArr = result[i].split(':');
                    candidates.push({
                        fromGroup:      +blockArr[0],
                        toGroup:        +blockArr[1],
                        hash:           blockArr[3],
                        timestamp:      +result[i + 1],
                        finder:         blockArr[4],
                        roundShares:    blockArr[5],
                        difficulty:     blockArr[6],

                        key:            result[i]
                    })
                }
            }

            _this.ProcessCandidates(candidates, function(blocks){
                _this.allocateRewards(blocks, _ => {});
            });

        })
    }

    this.ProcessCandidates = function(candidates, callback){
        var blocksNeedToReward = [];
        util.executeForEach(candidates, function(block, callback){
            const now = Date.now();

            const blockTimestampMs = block.timestamp * 1000;
            if (now < (blockTimestampMs + confirmationTime)){
                // the block reward might be locked, skip and
                // try to reward in the next loop
                callback();
                return;
            }

            _this.httpClient.blockInMainChain(block.hash, function(result){
                if (result.error){
                    logger.error('Check block in main chain error: ' + result.error);
                    callback();
                    return;
                }

                if (!result){
                    logger.error('Block is not in mainchain, remove block and shares, hash: ' + block.hash);
                    // var [fromGroup, toGroup] = util.blockChainIndex(Buffer.from(blockHash, 'hex'));
                    removeBlockAndShares2(block.fromGroup, block.toGroup, block.hash, block.key);
                    callback();
                    return;
                }

                _this.httpClient.getBlock(block.hash, function(result){
                    if (result.error){
                        logger.error('Get block error: ' + result.error + ', hash: ' + block.hash);
                        callback();
                        return;
                    }

                    handleBlock(result, function(data){
                        block.height = data.height;
                        block.rewardAmount = data.rewardAmount;
                        blocksNeedToReward.push(block);
                        callback();
                    });
                });
            });
            callback();
        }, _ => callback(blocksNeedToReward));
    }

    this.start = function(){
        scanBlocks();
        if (config.rewardEnabled){
            setInterval(scanBlocks, config.rewardInterval * 1000);
        }
    }
}
