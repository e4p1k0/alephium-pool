const Redis = require('ioredis');
const HttpClient = require('./httpClient');
const util = require('./util');

const ShareProcessor = module.exports = function ShareProcessor(config, logger) {
    const minConfirmationTime = config.minConfirmationTime * 1000;
    const confirmationTime = config.confirmationTime * 1000;
    const rewardPercent = 1 - config.withholdPercent;

    let poolType;
    if (!config.poolType || !['pplns', 'solo'].includes(config.poolType)) {
        poolType = 'pplns'; // default
    } else {
        poolType = config.poolType;
    }

    let pplns = 1;
    const defaultBaseShareDifficulty = 0.5;
    let baseShareDifficulty;

    if (poolType === 'pplns') {
        pplns = config.pplns ? config.pplns : 200000;

        const varDiffMinDiff = config?.pool?.varDiff?.minDiff;
        if (varDiffMinDiff) {
            baseShareDifficulty = varDiffMinDiff;
            logger.info( `Using baseShareDifficulty ${baseShareDifficulty}`);

        } else {
            logger.info(`Vardiff cfg not found, using default baseShareDifficulty ${defaultBaseShareDifficulty}`);
        }
    }

    const _this = this;
    const baseName = config.redis.baseName ? config.redis.baseName : 'alephium';
    this.redisClient = new Redis(config.redis.port, config.redis.host, {db: config.redis.db});
    this.httpClient = new HttpClient(config.daemon.host, config.daemon.port, config.daemon.apiKey);

    _this.handleShare = share => _this._handleShare(share);

    this.currentRoundKey = function (fromGroup, toGroup) {
        return baseName + ':' + fromGroup + ':' + toGroup + ':shares:currentRound';
    }

    this.roundKey = function (fromGroup, toGroup, blockHash) {
        return baseName + ':' + fromGroup + ':' + toGroup + ':shares:' + blockHash;
    }

    const hashrateKey = baseName + ':' + 'hashrate';
    const balancesKey = baseName + ':data:balances';

    this._handleShare = function (share) {
        const redisTx = _this.redisClient.multi();
        const currentMs = Date.now();
        const fromGroup = share.job.fromGroup;
        const toGroup = share.job.toGroup;
        const currentRound = _this.currentRoundKey(fromGroup, toGroup);
        redisTx.hincrbyfloat(currentRound, share.workerAddress, share.difficulty);

        const shareValue = share.difficulty;
        const baseShareDifficulty = 1;
        const times = Math.floor(share.difficulty / baseShareDifficulty);

        // console.log(`difficulty ${share.difficulty}, poolType ${poolType}, pplns ${pplns}, times ${times}, share.workerAddress ${share.workerAddress}`)

        for (let i = 0; i < times; ++i) {
            redisTx.lpush(baseName + ':' + 'lastShares', share.workerAddress);
        }
        redisTx.ltrim(baseName + ':' + 'lastShares', 0, pplns - 1);

        redisTx.hincrbyfloat(baseName + ':' + 'stats', 'roundShares', shareValue);
        redisTx.hincrbyfloat(baseName + ':' + 'miners' + ':' + share.workerAddress, 'roundShares', shareValue);

        const currentTs = Math.floor(currentMs / 1000);
        let worker = share.worker.replace(share.workerAddress, '').replace('.', '');
        if (!worker) {
            worker = 'default';
        }
        redisTx.zadd(hashrateKey, currentTs, [fromGroup, toGroup, share.workerAddress, worker, share.difficulty, currentMs].join(':'));
        redisTx.zadd(hashrateKey + ':' + share.workerAddress, currentTs, [fromGroup, toGroup, worker, share.difficulty, currentMs].join(':'));

        redisTx.exec(function (error, _) {
            if (error) logger.error('Handle share failed, error: ' + error);

            if (share.foundBlock) {
                PutBlock(share);
            }
        });
    }

    function PutBlock(share) {
        let baseRedisTx = _this.redisClient.multi();
        baseRedisTx.hgetall(baseName + ':' + 'stats');
        baseRedisTx.hgetall(baseName + ':' + 'miners' + ':' + share.workerAddress);
        baseRedisTx.hdel(baseName + ':' + 'stats', 'roundShares');
        baseRedisTx.hdel(baseName + ':' + 'miners' + ':' + share.workerAddress, 'roundShares');

        baseRedisTx.exec(function (err, res) {
            if (err) {
                logger.error('Get stats failed, error: ' + err);
            }
            // res [
            //     [
            //         null,
            //         {
            //             'height:0:1': '821446',
            //             'height:0:2': '818097',
            //             'height:0:3': '818432',
            //             'height:1:0': '817704',
            //             'height:1:2': '818494',
            //             'height:1:3': '818145',
            //             'height:2:0': '817579',
            //             'height:2:1': '816986',
            //             'height:2:3': '818056',
            //             'height:3:0': '818028',
            //             'height:3:1': '817704',
            //             'height:3:2': '817613',
            //             hashrate: '59802060000000',
            //             difficulty: '15205.982626104',
            //             lastBlockFound: '1690097163',
            //             roundShares: '27680.76111996000025428'
            //         }
            //     ],
            //         [
            //             null,
            //             {
            //                 balance: '0',
            //                 paid: '309.88980000000000001',
            //                 totalPayments: '106',
            //                 roundShares: '5843.43728915000003132'
            //             }
            //         ]
            //     ]

            const difficulty = res[0][1].difficulty | 0;
            const poolShares = res[0][1].roundShares | 0;
            const minerShares = res[1][1].roundShares | 0;

            let blockShares;
            if (poolType === 'solo') {
                blockShares = minerShares;
            } else {
                blockShares = poolShares;
            }

            const redisTx = _this.redisClient.multi();
            const currentMs = Date.now();
            const currentTs = Math.floor(currentMs / 1000);
            const blockHash = share.blockHash;
            const newKey = _this.roundKey(share.job.fromGroup, share.job.toGroup, blockHash);

            const currentRound = _this.currentRoundKey(share.job.fromGroup, share.job.toGroup);
            redisTx.rename(currentRound, newKey);
            redisTx.rename(baseName + ':' + 'lastShares', baseName + ':shares:' + share.blockHash);

            redisTx.hset(baseName + ':' + 'stats', 'lastBlockFound', currentTs);
            redisTx.zadd(baseName + ':' + 'blocks:candidates', currentTs, [
                share.job.fromGroup, share.job.toGroup,
                '', share.blockHash, share.workerAddress, blockShares, difficulty].join(':'));
            // '' is null height, we don`t know it right now

            redisTx.exec(function (error, _) {
                if (error) logger.error('Put block failed, error: ' + error);
            });
        });
    }

    function handleBlock(block, callback) {
        const transactions = block.transactions;
        const rewardTx = transactions[transactions.length - 1];
        const rewardOutput = rewardTx.unsigned.fixedOutputs[0];
        const blockData = {
            hash: block.hash,
            fromGroup: block.chainFrom,
            toGroup: block.chainTo,
            height: block.height,
            rewardAmount: rewardOutput.attoAlphAmount // string
        };
        callback(blockData);
    }

    // remove block shares and remove blockHash from pendingBlocks
    function removeBlockAndShares(block) {
        _this.redisClient
            .multi()
            .del(_this.roundKey(block.fromGroup, block.toGroup, block.hash))
            .zrem(baseName + ':blocks:candidates', block.key)
            .zadd(baseName + ':' + 'blocks:confirmed', block.timestamp, [
                block.fromGroup, block.toGroup,
                block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                block.rewardAmount, 1].join(':'))
            //  1 mean true, this is uncle/orphan or what we have in this blockchain

            .exec(function (error, _) {
                if (error) logger.error('Remove block shares failed, error: ' + error + ', blockHash: ' + block.hash);
            })
    }

    this.allocateRewards = function (blocks, callback) {
        const workerRewards = {};
        const redisTx = _this.redisClient.multi();
        util.executeForEach(blocks, function (block, callback) {
            allocateRewardForBlock(block, redisTx, workerRewards, callback);
        }, function (_) {
            for (let worker in workerRewards) {
                let workerRewardSum = 0;
                for (let num in workerRewards[worker]) {
                    const data = workerRewards[worker][num];

                    let rewardArr = [data.reward, data.share, data.hash, data.height];
                    if (poolType === 'solo') {
                        rewardArr = [...rewardArr, data.roundShares, data.difficulty];
                    }
                    const rewardData = rewardArr.join(':');
                    redisTx.zadd(baseName + ':rewards:' + worker, data.timestamp, rewardData);

                    workerRewardSum += data.reward;
                }
                redisTx.hincrbyfloat(baseName + ':miners:' + worker, 'balance', workerRewardSum);
                redisTx.hincrbyfloat(balancesKey, worker, workerRewardSum);
            }
            redisTx.exec(function (error, _) {
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

    function allocateRewardForBlock(block, redisTx, workerRewards, callback) {
        const round = _this.roundKey(block.fromGroup, block.toGroup, block.hash);
        const totalReward = Math.floor(parseInt(block.rewardAmount) * rewardPercent);

        if (poolType === 'pplns') {
            // logger.info(`Reward miners for block: ${block.hash} (${poolType.toUpperCase()} mode), total reward: ${totalReward}`);
            // logger.debug('Block hash: ' + block.hash + ', shares: ' + JSON.stringify(shares));
            _this.redisClient.hgetall(round, function (error, shares) {
                if (error) {
                    logger.error('Get shares failed, error: ' + error + ', round: ' + round);
                    callback();
                    return;
                }
                logger.info(`Reward miners for block: ${block.hash} (${poolType.toUpperCase()} mode), total reward: ${totalReward}`);
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
        } else if (poolType === 'solo') {
            logger.info(`Reward miners for block: ${block.hash} (${poolType.toUpperCase()} mode), total reward: ${totalReward}`);

            _this.allocateSoloReward(totalReward, workerRewards, block);
            console.log(block)
            redisTx.del(round);
            redisTx.zrem(baseName + ':blocks:candidates', block.key)
            redisTx.zadd(baseName + ':' + 'blocks:confirmed', block.timestamp, [
                block.fromGroup, block.toGroup,
                block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                block.rewardAmount, 0].join(':'));
            //  0 mean false, this is not uncle/orphan or what we have in this blockchain

            logger.info('Remove shares for block: ' + block.hash);
            callback();
        } else {    //  prop
            logger.error('Unsupported pool type!!!');
            _this.redisClient.hgetall(round, function (error, shares) {
                if (error) {
                    logger.error('Get shares failed, error: ' + error + ', round: ' + round);
                    callback();
                    return;
                }
                logger.info(`Reward miners for block: ${block.hash} (${poolType.toUpperCase()} mode), total reward: ${totalReward}`);
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
        }
    }

    this.allocateReward = function (totalReward, workerRewards, shares, block) {
        const totalShare = Object.keys(shares).reduce(function (acc, worker) {
            return acc + parseFloat(shares[worker]);
        }, 0);

        for (var worker in shares) {
            const percent = parseFloat(shares[worker]) / totalShare;
            const workerReward = util.toALPH(totalReward * percent);
            const rewardObj = {
                height: block.height,
                reward: workerReward,
                hash: block.hash,
                timestamp: block.timestamp,
                share: percent,
                // roundShares:    block.roundShares,
                // difficulty:     block.difficulty
            };
            if (workerRewards[worker]) {
                workerRewards[worker].push(rewardObj)
            } else {
                workerRewards[worker] = [rewardObj]
            }
        }
    }

    this.allocateSoloReward = function (totalReward, workerRewards, block) {
        const rewardObj = {
            height: block.height,
            reward: util.toALPH(totalReward),
            hash: block.hash,
            timestamp: block.timestamp,
            share: rewardPercent,
            roundShares: block.roundShares,
            difficulty: block.difficulty
        };
        if (workerRewards[block.finder]) {
            workerRewards[block.finder].push(rewardObj)
        } else {
            workerRewards[block.finder] = [rewardObj]
        }
    }

    function scanBlocks() {
        _this.redisClient.zrangebyscore(baseName + ':' + 'blocks:candidates', '-inf', '+inf', 'WITHSCORES', function (err, result) {
            if (err) {
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
                        fromGroup: +blockArr[0],
                        toGroup: +blockArr[1],
                        height: blockArr[2],
                        hash: blockArr[3],
                        timestamp: +result[i + 1],
                        finder: blockArr[4],
                        roundShares: blockArr[5],
                        difficulty: blockArr[6],

                        key: result[i]
                    })
                }
            }

            _this.ProcessCandidates(candidates, function (blocks) {
                _this.allocateRewards(blocks, _ => {
                });
            });

        })
    }

    this.ProcessCandidates = function (candidates, callback) {
        let blocksNeedToReward = [];
        util.executeForEach(candidates, function (block, callback) {
            const now = Date.now();

            const blockTimestampMs = block.timestamp * 1000;

            const onlyGetAdditionalData = !block.height && now > (blockTimestampMs + minConfirmationTime);
            const blockIsConfirmed = now > (blockTimestampMs + confirmationTime);

            if (!blockIsConfirmed && !onlyGetAdditionalData) {
                const diff = (blockTimestampMs + confirmationTime - now) / 1000;
                logger.info(`Block ${block.hash} is not ready (${diff} seconds left)`);
                // the block reward might be locked, skip and
                // try to reward in the next loop
                callback();
                return;
            }

            _this.httpClient.blockInMainChain(block.hash, function (result) {
                if (result.error) {
                    logger.error('Check block in main chain error: ' + result.error);
                    callback();
                    return;
                }

                if (!result) {
                    if (blockIsConfirmed) {
                        logger.error('Block is not in mainchain, remove block and shares, hash: ' + block.hash);
                        removeBlockAndShares(block);
                    }

                    callback();
                    return;
                }

                _this.httpClient.getBlock(block.hash, function (result) {
                    if (result.error) {
                        logger.error('Get block error: ' + result.error + ', hash: ' + block.hash);
                        callback();
                        return;
                    }

                    handleBlock(result, function (data) {
                        block.height = data.height;
                        block.rewardAmount = data.rewardAmount;

                        if (onlyGetAdditionalData) {
                            let redisTx = _this.redisClient.multi();
                            redisTx.zrem(baseName + ':blocks:candidates', block.key)
                            redisTx.zadd(baseName + ':' + 'blocks:candidates', block.timestamp, [
                                block.fromGroup, block.toGroup,
                                block.height, block.hash, block.finder, block.roundShares, block.difficulty,
                                block.rewardAmount, 0].join(':'));
                            redisTx.exec(function (error, _) {
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

    this.start = function () {
        scanBlocks();
        if (config.rewardEnabled) {
            setInterval(scanBlocks, config.rewardInterval * 1000);
        }
    }
};
