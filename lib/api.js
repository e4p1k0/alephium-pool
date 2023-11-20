const Redis = require('ioredis');
const HttpClient = require('./httpClient');

var Api = module.exports = function(config, logger){
    var _this = this;
    const baseName = config.redis.baseName ? config.redis.baseName : 'alephium';

    this.redisClient = new Redis(config.redis.port, config.redis.host, {db: config.redis.db});
    this.httpClient = new HttpClient(config.daemon.host, config.daemon.port, config.daemon.apiKey);

    this.getHeight = function(){
        for (let fromGroup in [0, 1, 2, 3]) {
            for (let toGroup in [0, 1, 2, 3]) {

                _this.httpClient.chainInfo(fromGroup, toGroup, function(result){
                    if (result.error){
                        logger.error('Api request failed, error: ' + result.error);
                        return;
                    }
                    if (! ('currentHeight' in result)) {
                        logger.error('No currentHeight field in api request result');
                        return;
                    }

                    const chainHeight = parseFloat(result.currentHeight);
                    const key = 'height' + ':' + fromGroup + ':' + toGroup;
                    _this.redisClient.hset(baseName + ':' + 'stats', key, chainHeight);
                })
            }
        }
    }

    this.getHashrate = function(){
        this.httpClient.currentHashrate(function(result){
            if (result.error){
                logger.error('Api request failed, error: ' + result.error);
                return;
            }
            if (! ('hashrate' in result)) {
                logger.error('No hashrate field in api request result');
                return;
            }

            let rawHr = result.hashrate
            let hr = parseFloat(rawHr.split(' ')[0]) * 1000000;

            _this.redisClient.hset(baseName + ':' + 'stats', 'hashrate', hr)
        })
    }

    this.getDifficulty = function(){
        this.httpClient.currentDifficulty(function(result){
            if (result.error){
                logger.error('Api request failed, error: ' + result.error);
                return;
            }
            if (! ('difficulty' in result)) {
                logger.error('No difficulty field in api request result');
                return;
            }

            const difficulty = result.difficulty / 1000000000;

            _this.redisClient.hset(baseName + ':' + 'stats', 'difficulty', difficulty)
        })
    }

    this.runOnce = function() {
        _this.getHeight();
        _this.getHashrate();
        _this.getDifficulty();
    }

    this.reportStatsRegularly = function(){
        // first call on start
        _this.runOnce()

        setInterval(function(){
            _this.runOnce()
        }, config.apiInterval * 1000);
    }
}
