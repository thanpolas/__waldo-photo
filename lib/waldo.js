/*
 * Waldo Exercise
 *
 */

var Promise = require('bluebird');
var cip = require('cip');
var Aws = require('aws-sdk');
var ExifImage = require('exif').ExifImage;
var redis = require('redis');


/**
 * The master Ctor to process photos from S3.
 *
 * @constructor
 */
var PhotoProcess = module.exports = cip.extend(function(bucketName) {
  /** @type {string} The s3 bucket name to scrape */
  this.bucketName = bucketName;

  /** @type {Redis} Redis client */
  this.redisClient = null;

  /** @type {?Function} Promisified redis command */
  this.__redisSet = null;
  /** @type {?Function} Promisified redis command */
  this.__redisGet = null;

  /** @type {Aws.S3} S3 SDK Instance */
  this.s3 = new Aws.S3({
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY,
      secretAccessKey: process.env.AWS_SECRET_KEY,
    },
    region: process.env.AWS_REGION || 'us-west-2',
  });

  // promisify needed methods
  this.s3ListObjectsV2 = Promise.promisify(this.s3.listObjectsV2.bind(this.s3));
  this.s3getObject = Promise.promisify(this.s3.getObject.bind(this.s3));

  /** @type {Array.<Object>} The list of s3 objects in the bucket */
  this.bucketContents = [];
});

/** @const {Number} How many concurrent s3 downloads to allow */
PhotoProcess.S3_CONCURRENT_DOWNLOAD = 4;
/** @const {Number} How many concurrent Redis writes to allow */
PhotoProcess.REDIS_CONCURRENCY = 10;

/**
 * The main operation entry point.
 *
 * @param {boolean=} isDebut Set to true for debug print at the end.
 * @return {Promise} A Promise.
 */
PhotoProcess.prototype.run = Promise.method(function(isDebug) {

  return this._redisInit()
    .bind(this)
    .then(this._listObjects)
    .then(this._fetchObjects)
    .then(this._storeExifData)
    .then(function(res) {
      console.log('All done,', res.length, 'total photos stored.');

      if (!isDebug) {
        return;
      }

      return Promise.resolve(res)
        .bind(this)
        .map(function(item) {
          return this._redisGet(item.key)
            .then(function(res) {
              console.log('Redis response for key:', item.key, '::', res);
            });
        }, {concurrency: PhotoProcess.REDIS_CONCURRENCY});
    });
});

/**
 * Fetch all the objects for the provided bucket.
 *
 * @return {Promise} A Promise.
 * @private
 */
PhotoProcess.prototype._listObjects = Promise.method(function() {
  var params = {
    Bucket: this.bucketName,
  };

  return this.s3ListObjectsV2(params)
    .bind(this)
    .then(function(res) {
      if (res && Array.isArray(res.Contents) && res.Contents.length) {
        this.bucketContents = res.Contents;
      }
    });
});

/**
 * Fetch all the objects from S3.
 *
 * @return {Promise} A Promise.
 * @private
 */
PhotoProcess.prototype._fetchObjects = Promise.method(function() {
  return Promise.resolve(this.bucketContents)
    .bind(this)
    .map(this._getAndExif, {concurrency: PhotoProcess.S3_CONCURRENT_DOWNLOAD});
});

/**
 * Fetch the object from S3 and pass it through exif.
 *
 * @param {Object} item Item fetched from s3 list bucket.
 * @return {Promise(Array.<Object>)} A Promise with the results.
 * @private
 */
PhotoProcess.prototype._getAndExif = Promise.method(function(item, index) {
  var key = item.Key;

  console.log('fetching and processing:', index, key);

  return this._getObject(key)
    .bind(this)
    .then(this._getExif)
    .then(function(exifData) {
      // normalize the data
      return {
        key: key,
        exif: exifData,
      };
    });
});

/**
 * Fetch a single object from S3.
 *
 * @param {string} key The object key.
 * @return {Promise(Object)} A Promise with the result.
 * @private
 */
PhotoProcess.prototype._getObject = Promise.method(function(key) {
  var params = {
    Bucket: this.bucketName,
    Key: key,
    // only need first few bytes to read the exif header
    Range: '0:4096',
  };

  return this.s3getObject(params);
});

/**
 * Get exif headers out of the s3 fetching stream.
 *
 * @param {Object} s3Res The s3 response from getObject op.
 * @return {Promise(Object)} A Promise with the exif headers.
 * @private
 */
PhotoProcess.prototype._getExif = Promise.method(function(s3Res) {
  return new Promise(function(resolve) {
    try {
      new ExifImage(s3Res.Body, function (error, exifData) {
        if (error) {
          console.error('Exif OP Error: ' + error.message);
          resolve(null);
          return;
        }

        resolve(exifData);
      });
    } catch (error) {
      console.error('Exif catch Error: ' + error.message);
      resolve(null);
    }
  });
});

/**
 * Store the exif data using the object key name as index.
 *
 * @param {Array.<Object>} results The exif results.
 * @return {Promise(Array)} A Promise.
 * @private
 */
PhotoProcess.prototype._storeExifData = Promise.method(function(results) {
  return Promise.resolve(results)
    .bind(this)
    .map(this._storeExif, {concurrency: PhotoProcess.REDIS_CONCURRENCY});
});

/**
 * Manages storing to redis of a single item.
 *
 * @param {Object} item The item containing keys "key" and "exif".
 * @return {Promise(Object)} A Promise relaying the item.
 * @private
 */
PhotoProcess.prototype._storeExif = Promise.method(function(item) {
  return this._redisSet(item.key, item.exif)
    .return(item);
});

/**
 * Create a redis client.
 *
 * @return {Promise} A Promise.
 * @private
 */
PhotoProcess.prototype._redisInit = Promise.method(function() {
  if (this.redisClient) {
    return;
  }
  this.redisClient = redis.createClient();
  this.__redisGet = Promise.promisify(this.redisClient.get.bind(this.redisClient));
  this.__redisSet = Promise.promisify(this.redisClient.set.bind(this.redisClient));
});

/**
 * Set a value to redis store.
 *
 * @param {string} key The desired key.
 * @param {*} value The value.
 * @return {Promise(*)} A Promise with the response.
 * @private
 */
PhotoProcess.prototype._redisSet = Promise.method(function(key, value) {
  if (typeof value !== 'string') {
    value = JSON.stringify(value);
  }

  return this.__redisSet(key, value);
});

/**
 * Get a value from redis store.
 *
 * @param {string} key The desired key.
 * @return {Promise(*)} A Promise with the response.
 * @private
 */
PhotoProcess.prototype._redisGet = Promise.method(function(key) {
  return this.__redisGet(key)
    .bind(this)
    .then(function(res) {
      return JSON.parse(res);
    });
});

var photoProcess = new PhotoProcess('waldo-recruiting');

photoProcess.run();

