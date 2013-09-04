var knox  = require('knox');
var mpu   = require('knox-mpu');
var retry = require('retry');

exports = S3;

S3.PUT_TIMEOUTS = [500, 500, 500, 1500, 3000];
S3.MAX_SIZE   = 5 * 1024 * 1024 * 1024 - 1;

function S3(options) {
  this.endpoint = options.host || 's3.amazonaws.com';

  this.key    = options.key;
  this.secret = options.secret;
  this.bucket = options.bucket;

  this.s3Client = knox.createClient({
    key     : this.key,
    secret  : this.secret,
    bucket  : this.bucket,
    endpoint: this.endpoint
  });

  this.input     = options.input;
  this.headers   = options.headers;
  this.acl       = options.acl;
  this.path      = this.options.path;
  this.batchSize = options.batchSize || 1;

  this.urlPrefix = options.urlPrefix;
}

S3.prototype.put = function(cb) {
  var put = retry.operation(S3.PUT_TIMEOUTS);
  var self = this;

  put.attempt(function() {
    if (self.input.size < S3.MAX_SIZE) {
      self.s3Client.putFile(
        self.input.path, self.path,
        self._handleCallback.bind(self, put, cb)
      );
    } else {
      self.multipartUpload(self._handleCallback.bind(self, put, cb));
    }
  });
};

S3.prototype.multipartUpload = function(cb) {
  if (!this.headers) {
    this.headers = {};
  }

  if (this.acl !== undefined) {
    this.headers['x-amz-acl'] = options.acl;
  } else {
    this.headers['x-amz-acl'] = 'public-read';
  }

  var mpuOpts = {
    client:     this.s3Client,
    objectName: this.path,
    file:       this.input.path,
    headers:    this.headers,
    batchSize:  this.batchSize
  };
  var self = this;

  try {
    var upload = new MultiPartUpload(mpuOpts, function(err, body) {
      if (err) {
        return cb(err);
      }

      var prefix = self.urlPrefix;
      if (!prefix) {
        prefix = 'http://' + self.bucket + '.' + self.endpoint + '/';
      }

      cb(null, prefix + self.path);
    });
  } catch(e) {
    cb(new Error('Multipart Upload failed'));
  }
};

S3.prototype._handleCallback = function(put, cb, err) {
  if (err) {
    this._handlePutError(err, put, cb);
    return;
  }
  cb();
};

S3.prototype._handlePutError = function(err, put, cb) {
  var denied = 'access_denied';
  if (err.message && err.message.match(/400/)) {
    err.type = denied;
  }

  if (err.stderr && err.stderr.match(/403/)) {
    err.type = denied;
  }

  var s3AccessDenied = err && err.type === denied;
  if (!s3AccessDenied && put.retry(err)) {
    return;
  }

  var finalErr = s3AccessDenied ? err : put.mainError();
  cb(finalErr);
};
