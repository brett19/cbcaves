"use strict";

function ConfigGenerator(cluster, bucket) {
  this.cluster = cluster;
  this.bucket = bucket;
}

ConfigGenerator.prototype._path = function(path) {
  path = path.replace(/:name/g, this.bucket.name);
  path = path.replace(/:uuid/g, this.bucket.uuid);
  return '/pools/default' + path;
};

ConfigGenerator.prototype._generateVbMap = function() {
  var config = {
    'hashAlgorithm': 'CRC',
    'numReplicas': this.bucket.numReplicas,
    'serverList': [],
    'sslServerList': [],
    'vBucketMap': []
  };

  for (var nId = 0; nId < this.nodes.length; ++nId) {
    var node = this.nodes[nId];
    config.serverList.push('127.0.0.1:' + node.memdSvc.port);
    config.sslServerList.push('127.0.0.1:' + node.memdSvc.sslPort);
  }

  for (var vbId = 0; vbId < this.bucket.vbMap.length; ++vbId) {
    var vb = this.bucket.vbMap[vbId];

    var vbEntry = [];
    for (var repId = 0; repId < vb.length; ++repId) {
      var vbNode = this.nodeById(vb[repId]);
      vbEntry.push(this.nodes.indexOf(vbNode));
    }
    config.vBucketMap.push(vbEntry);
  }

  return config;
};

ConfigGenerator.prototype._generateNodeConfig = function(node) {
  var config = {
    'couchbaseApiBaseHTTPS': 'https://' + node.host + ':' + node.capiSvc.sslPort + '/' + this.bucket.name,
    'couchbaseApiBase': 'http://' + node.host + ':' + node.capiSvc.port + '/' + this.bucket.name,
    'systemStats': {
      'cpu_utilization_rate': 0,
      'swap_total': 2048 * 1024 * 1024,
      'swap_used': 0,
      'mem_total': 4096 * 1024 * 1024,
      'mem_free': 4096 * 1024 * 1024
    },
    'interestingStats': {
      'cmd_get': 0,
      'couch_docs_actual_disk_size': 0,
      'couch_docs_data_size': 0,
      'couch_views_actual_disk_size': 0,
      'couch_views_data_size': 0,
      'curr_items': 0,
      'curr_items_tot': 0,
      'ep_bg_fetched': 0,
      'get_hits': 0,
      'mem_used': 0,
      'ops': 0,
      'vb_replica_curr_items': 0
    },
    'uptime': '1000',
    'memoryTotal': 4096 * 1024 * 1024,
    'memoryFree': 3072 * 1024 * 1024,
    'mcdMemoryReserved': 3164,
    'mcdMemoryAllocated': 3164,
    'replication': 0,
    'clusterMembership': 'active',
    'status': 'healthy',
    'optNode': 'ns_1@' + node.host,
    'hostname': node.host + ':' + node.mgmtSvc.port,
    'clusterCompatibility': 131077,
    'version': '3.0.0-490-rel-enterprise',
    'os': 'x86_64-unknown-linux-gnu',
    'ports': {
      'sslProxy': 0,
      'httpsMgmt': node.mgmtSvc.sslPort,
      'httpsCAPI': node.capiSvc.sslPort,
      'sslDirect': node.memdSvc.sslPort,
      'proxy': 0,
      'direct': node.memdSvc.port
    }
  };
  return config;
};

ConfigGenerator.prototype.generateConfig = function() {
  var config = {
    'name': this.bucket.name,
    'bucketType': 'membase',
    'authType': 'sasl',
    'saslPassword': '',
    'proxyPort': 0,
    'replicaIndex': false,
    'uri': this._path('/buckets/:name?bucket_uuid=:uuid'),
    'streamingUri': this._path('/bucketsStreaming/:name?bucket_uuid=:uuid'),
    'localRandomKeyUri': this._path('/buckets/:name/localRandomKey'),
    'controllers': {
      'flush': this._path('/buckets/:name/doFlush'),
      'compactAll': this._path('/buckets/:name/compactBucket'),
      'compactDB': this._path('/buckets/:name/compactDatabases'),
      'purgeDeletes': this._path('/buckets/:name/unsafePurgeBucket'),
      'startRecovery': this._path('/buckets/:name/startRecovery')
    },
    'nodes': [],
    'stats': {
      'uri': this._path('/buckets/:name/stats'),
      'directoryURI': this._path('/buckets/:name/statsDirectory'),
      'nodeStatsListURI': this._path('/buckets/:name/nodes')
    },
    'ddocs': {
      'uri': this._path('/buckets/:name/ddocs')
    },
    'nodeLocator': 'vbucket',
    'fastWarmupSettings': false,
    'autoCompactionSettings': false,
    'uuid': this.bucket.uuid,
    'vBucketServerMap': this._generateVbMap(),
    'replicaNumber': 1,
    'threadsNumber': 3,
    'quota': {
      'ram': 3072 * 1024 * 1024,
      'rawRAM': 1024 * 1024 * 1024
    },
    'basicStats': {
      'quotaPercentUsed': 0,
      'opsPerSec': 0,
      'diskFetches': 0,
      'itemCount': 0,
      'diskUsed': 0,
      'dataUsed': 0,
      'memUsed': 0
    },
    'bucketCapabilitiesVer': '',
    'bucketCapabilities': [
      'touch',
      'couchapi'
    ]
  };
  for (var i = 0; i < this.cluster.nodes.length; ++i) {
    config.nodes.push(this._generateNodeConfig(this.cluster.nodes[i]));
  }
  return config;
};

ConfigGenerator.generateConfig = function(cluster, bucket) {
  return (new ConfigGenerator(cluster, bucket)).generateConfig();
};

ConfigGenerator.generateTerseConfig = function(cluster, bucket) {
  throw 'not yet supported';
};

module.exports = ConfigGenerator;
