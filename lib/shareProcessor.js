const Redis = require('ioredis');
const HttpClient = require('./httpClient');
const util = require('./util');

var ShareProcessor = module.exports = function ShareProcessor(config, logger){
    const minConfirmationTime = config.minConfirmationTime * 1000;
    const confirmationTime = config.confirmationTime * 1000;
    const rewardPercent = 1 - config.withholdPercent;

    let poolType;
    if (!config.poolType || !['prop', 'solo'].includes(config.poolType)) {
        poolType = 'prop'; // default
    } else {
        poolType = config.poolType;
    }

    var _this = this;
    const baseName = config.redis.baseName ? config.redis.baseName : 'alephium';
    this.redisClient = new Redis(config.redis.port, config.redis.host, {db: config.redis.db});
    this.httpClient = new HttpClient(config.daemon.host, config.daemon.port, config.daemon.apiKey);

    _this.handleShare = share => _this._handleShare(share);

    this.currentRoundKey = function(fromGroup, toGroup){
        return baseName + ':' + fromGroup + ':' + toGroup + ':shares:currentRound';
    }

    this.roundKey = function(fromGroup, toGroup, blockHash){
        return baseName + ':' + fromGroup + ':' + toGroup + ':shares:' + blockHash;
    }

    const hashrateKey = baseName + ':' + 'hashrate';
    const balancesKey = baseName + ':data:balances';

    this._handleShare = function(share){
        var redisTx = _this.redisClient.multi();
        var currentMs = Date.now();
        var fromGroup = share.job.fromGroup;
        var toGroup = share.job.toGroup;
        var currentRound = _this.currentRoundKey(fromGroup, toGroup);
        redisTx.hincrbyfloat(currentRound, share.workerAddress, share.difficulty);

        const shareValue = share.difficulty;
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
                PutBlock(share);
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

            var redisTx = _this.redisClient.multi();
            var currentMs = Date.now();
            var currentTs = Math.floor(currentMs / 1000);
            var blockHash = share.blockHash;
            var newKey = _this.roundKey(share.job.fromGroup, share.job.toGroup, blockHash);

            var currentRound = _this.currentRoundKey(share.job.fromGroup, share.job.toGroup);
            redisTx.rename(currentRound, newKey);

            redisTx.hset(baseName + ':' + 'stats', 'lastBlockFound', currentTs);
            redisTx.hdel(baseName + ':' + 'stats', 'roundShares');

            redisTx.zadd(baseName + ':' + 'blocks:candidates', currentTs, [
                share.job.fromGroup, share.job.toGroup,
                '', share.blockHash, share.workerAddress, roundShares, difficulty].join(':'));
            // '' is null height, we don`t know it right now

            redisTx.exec(function(error, _){
                if (error) logger.error('Put block failed, error: ' + error);
            });
        })
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
    function removeBlockAndShares(block){
        _this.redisClient
            .multi()
            .del(_this.roundKey(block.fromGroup, block.toGroup, block.hash))
            .zrem(baseName + ':blocks:candidates', block.key)
            .zadd(baseName + ':' + 'blocks:confirmed', block.timestamp, [
                block.fromGroup, block.toGroup,
                block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                block.rewardAmount, 1].join(':'))
                //  1 mean true, this is uncle/orphan or what we have in this blockchain

            .exec(function(error, _){
                if (error) logger.error('Remove block shares failed, error: ' + error + ', blockHash: ' + block.hash);
            })
    }

    this.allocateRewards = function(blocks, callback){
        var workerRewards = {};
        var redisTx = _this.redisClient.multi();
        util.executeForEach(blocks, function(block, callback){
            allocateRewardForBlock(block, redisTx, workerRewards, callback);
        }, function(_){
            for (let worker in workerRewards){
                let workerRewardSum = 0;
                for (let num in workerRewards[worker]) {
                    const data = workerRewards[worker][num];

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
        var round = _this.roundKey(block.fromGroup, block.toGroup, block.hash);
        var totalReward = Math.floor(parseInt(block.rewardAmount) * rewardPercent);

        if (poolType === 'prop') {
            _this.redisClient.hgetall(round, function (error, shares) {
                if (error) {
                    logger.error('Get shares failed, error: ' + error + ', round: ' + round);
                    callback();
                    return;
                }

                logger.info('Reward miners for block: ' + block.hash + ', total reward: ' + totalReward);
                logger.debug('Block hash: ' + block.hash + ', shares: ' + JSON.stringify(shares));
                _this.allocateReward(totalReward, workerRewards, shares, block);

                redisTx.del(round);
                redisTx.zrem(baseName + ':blocks:candidates', block.key)
                redisTx.zadd(baseName + ':' + 'blocks:confirmed', block.timestamp, [
                    block.fromGroup, block.toGroup,
                    block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                    block.rewardAmount, 0].join(':'));
                //  0 mean false, this is not uncle/orphan or what we have in this blockchain

                logger.info('Remove shares for block: ' + block.hash);
                callback();
            });
        } else {
            logger.info('Reward miners for block: ' + block.hash + ', total reward: ' + totalReward);
            // logger.debug('Block hash: ' + block.hash + ', shares: ' + JSON.stringify(shares));
            _this.allocateSoloReward(totalReward, workerRewards, block);

            redisTx.del(round);
            redisTx.zrem(baseName + ':blocks:candidates', block.key)
            redisTx.zadd(baseName + ':' + 'blocks:confirmed', block.timestamp, [
                block.fromGroup, block.toGroup,
                block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                block.rewardAmount, 0].join(':'));
            //  0 mean false, this is not uncle/orphan or what we have in this blockchain

            logger.info('Remove shares for block: ' + block.hash);
            callback();
        }
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
            if (workerRewards[worker]){
                workerRewards[worker].push(rewardObj)
            } else {
                workerRewards[worker] = [rewardObj]
            }
        }
    }

    this.allocateSoloReward = function(totalReward, workerRewards, block){
        const rewardObj = {
            height:     block.height,
            reward:     util.toALPH(totalReward),
            hash:       block.hash,
            timestamp:  block.timestamp,
            share:      rewardPercent
        };
        if (workerRewards[block.finder]){
            workerRewards[block.finder].push(rewardObj)
        } else {
            workerRewards[block.finder] = [rewardObj]
        }
    }

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
                        height:         blockArr[2],
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
        let blocksNeedToReward = [];
        util.executeForEach(candidates, function(block, callback){
            const now = Date.now();

            const blockTimestampMs = block.timestamp * 1000;

            const onlyGetAdditionalData = !block.height && now > (blockTimestampMs + minConfirmationTime);
            const blockIsConfirmed = now > (blockTimestampMs + confirmationTime);

            if (!blockIsConfirmed && !onlyGetAdditionalData){
                const diff = (blockTimestampMs + confirmationTime - now) / 1000;
                logger.info(`Block ${block.hash} is not ready (${diff} seconds left)`);
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
                    if (blockIsConfirmed) {
                        logger.error('Block is not in mainchain, remove block and shares, hash: ' + block.hash);
                        removeBlockAndShares(block);
                    }

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

                        if (onlyGetAdditionalData) {
                            let redisTx = _this.redisClient.multi();
                            redisTx.zrem(baseName + ':blocks:candidates', block.key)
                            redisTx.zadd(baseName + ':' + 'blocks:candidates', block.timestamp, [
                                block.fromGroup, block.toGroup,
                                block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                                block.rewardAmount, 0].join(':'));
                            redisTx.exec(function(error, _){
                                if (error) {
                                    logger.error('Error while adding data to candidate block ' + error);
                                }
                            });
                            logger.info(`Additional info for block ${block.hash} recorded`);
                        } else {
                            blocksNeedToReward.push(block);
                        }

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
