# Test Options

The following is a list of options which can be provided via annotations on a
test.

```
@test      - Specifies the name of a test
@needs     - Specifies a needed capability option
@group     - Specifies a group name for this test to be a part of (ie: slow)
@timeout   - Specifies a non-default timeout for this test
```

# Capability Options

The following is a list of options which are allowed from a `hello` operation sent from an SDK harness.  Note that these options are also available to be checked from using the `@needs` test option.

```
couchbase       - Supports Couchbase Buckets
memcached       - Supports Memcached Buckets
cccp_config     - Supports config via CCCP
http_config     - Supports config via HTTP
ssl             - Supports SSL
observe         - Supports the OBSERVE operation
endure          - Supports the ENDURE operation
op_durability   - Supports durability requirements on operations
bigint          - Supports 64-bit integers (INCR/DECR)
multi_op        - Supports pipelining operations
config_cache    - Supports configuration cacheing
replica_get     - Supports retrieving values from replicas
binary_key      - Supports binary keys
async           - Supports async operations
```

The following options are allowed to be used from the `@needs` test option.

```
mock            - Executing against a mock cluster
real            - Executing against a real cluster
```
