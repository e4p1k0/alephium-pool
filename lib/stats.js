const Redis = require('ioredis');

var Stats = module.exports = function(config, logger){
    var _this = this;
    const baseName = config.redis.baseName ? config.redis.baseName : 'alephium';
    var hashrateKey = baseName + ':' + 'hashrate';
    this.redisClient = new Redis(config.redis.port, config.redis.host, {db: config.redis.db});

    function calcHashrate(interval, callback){
        var currentTs = Math.floor(Date.now() / 1000);
        var from = currentTs - interval;

        let miners = []

        // console.log('from', from)
        // console.log('to  ', currentTs)
        _this.redisClient
            .multi()
            .zrangebyscore(hashrateKey, from, '+inf')
            .zremrangebyscore(hashrateKey, '-inf', '(' + from)
            .exec(function(error, results){
                if (error){
                    logger.error('Get hashrate data failed, error: ' + error);
                    callback({error: error});
                    return;
                }

                let diffSum = 0;
                for (const string of results[0][1]){
                    let shareArr = string.split(':');
                    // 1                                                                //  0   fromGroup
                    // :1                                                               //  1   toGroup
                    // :1EeV19mhjsRzpJK2n3qNwvWAPimjY5LhbVqRT5QKK87Ve                   //  2   Address
                    // :Laptop4                                                         //  3   Worker
                    // :1.63043478                                                      //  4   difficulty
                    // :1686293339694                                                   //  5   Timestamp
                    let diff = parseFloat(shareArr[4]);
                    diffSum += diff;

                    let index = miners.findIndex(el => el.address === shareArr[2]);
                    if (index === -1) {
                        let miner = {
                            address: shareArr[2],
                            diffSum: diff,
                        }

                        index = miners.length;
                        miners.push(miner);
                    } else {
                        miners[index].diffSum += diff;
                    }

                }

                for (let miner of miners) {
                    const hashrate = miner.diffSum * 16 * Math.pow(2, config.diff1TargetNumZero) / interval;
                    _this.redisClient.zadd(baseName + ':' + 'charts' + ':' + miner.address, currentTs, [hashrate].join(':'));
                }

                // multiply 16 because we encoded the chainIndex to blockHash
                const hashrate = diffSum * 16 * Math.pow(2, config.diff1TargetNumZero) / interval;
                const hashrateMHs = (hashrate / 1000000).toFixed(2);
                _this.redisClient.zadd(baseName + ':' + 'charts' + ':' + 'pool', currentTs, [hashrate].join(':'));
                callback({hashrate: hashrateMHs});
            });
    }

    this.getStats = function(interval, callback){
        calcHashrate(interval, callback);
    }

    this.runOnce = function(interval) {
        _this.getStats(config.window, function(result){
            if (result.error){
                logger.error('Stats failed, error: ' + result.error);
                return;
            }
            logger.info('Pool hashrate: ' + result.hashrate + ' MH/s');
        })
    }

    this.reportStatsRegularly = function(){
        // first call on start
        _this.runOnce(config.window)

        setInterval(function(){
            _this.runOnce(config.window)
        }, config.statsCollectionInterval * 1000);
    }
}
