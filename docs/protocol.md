# Protocol

## General

The protocol consists of sending JSON objects prefixed with a 32 bit integer
representing the length of the upcoming JSON string.  These JSON objects come in
two formats, either requests or responses.

## Requests

Requests contain a sequence  number `seqno` (which may be 0 for operations which
do not generate responses) as well as an operation type `op` and lastly an
object representing the options for the operation `opts`.  The following is an
example of a request object.

```json
{
  "seqno": 1,
  "op": "hello",
  "opts": {
    "version": 1,
    "platform": "macos",
    "architecture": "x86_64",
    "clientVersion": "1.0.0",
    "clientBuild": "1.0.0-master",
    "supports": [
      "couchbase",
      "memcached",
      "ssl",
      "observe"
    ]
  }
}
```

## Responses

Responses contain a sequence number `seqno` which is expected to correspond
directly to the request which it is a response to.  Additionally they should
contain a result object `result` representing the result of the operation (this
value may be null or missing in case of an operation which fails entirely).
Lastly there may be an `error` value included which should match one of the
error types as defined in the error types section below.  Note also that an
`errorText` field may be included to enhance the error type information with
client-specific or simply more detailed error information to show in the CAVES
log.

Example Response:
```json
{
  "seqno": 1,
  "error": 1,
  "errorText": "we freaked out and decided it was a bad idea",
  "result": null
}
```

## Key / Value Data

In order to support multiple different formats of data being passed to the clients via JSON, the keys and values are passed using a special structure.  This structure contains a `type` field which can either be `binary`, `json` or `string`.  In addition to the `type` field, a `value` field will also exist, and potentially a `count`.  In the case of a `binary` type, the `value` field will be a base64 encoded string representing a sequence of binary bytes.  In the case of a `string` type, the `value` field will contain a normal string.  In the case of JSON, the value field will contain a JSON encoded string, your MUST decode this string, and pass the decoded object to the SDK if supported.

Example (JSON):
```json
{
  "type": "json",
  "value": "{\"x\":\"habbo!\"}"
}
```
```json
{
  "x": "habbo!"
}
```

Example (String):
```json
{
  "type": "string",
  "value": "joy",
  "count": 4
}
```
```
joyjoyjoyjoy
```

Example (Binary):
```json
{
  "type": "binary",
  "value": "aGVsbG8=",
  "count": 2
}
```
```
\x68\x65\x6C\x6C\x6F\x68\x65\x6C\x6C\x6F
```


## Error Codes

Error codes are specific to the CAVES system and do not reflect any error codes
you would expect to see from memcached or any other Couchbase system.  The
purpose of these error codes is to provide a consistent list of errors which any
client can expose, regardless of the internals of that client.  Note that in the
case that there are no direct map of a client error code to a CAVES code, you
should use the most specific generic error code as possible.  Note that in some
cases, using a less specific error code may cause a test to fail to pass, note
that this will only happen in the case that the it has been agreed that an error
must have a certain degree of specificity regardless of SDK.  We also provide a
range of error codes which can be specified which are client specific, however
receiving these errors during tests which are not client-specific (ie. no
`@needs sdk_node`) will trigger warnings and cause the test to fail.

The following is a current list of error codes:

Code  | Description
----- | --------------
0+    | CAVES Issues
1     | CAVES Unknown Operation
2     | CAVES Invalid Arguments
100+  | SDK Issues
100   | Generic SDK Issue
101   | Temporary Error
102   | Invalid Arguments
200+  | Server Issues
200   | Key Not Found
201   | Key Already Exists
202   | Durability Requirements Failed
4000+ | Client Specific Error Codes

##### CAVES Unknown Operation

Should be sent if an operation is received from CAVES which the harness does not know how to handle correctly.

##### CAVES Invalid Arguments

An error occurred due to the harness not being able to successfully parse a known CAVES operation due to invalid arguments or otherwise.

##### Generic SDK Issue

Should be sent if an error occurs which is due to a client issue (ie: scheduling error).

##### Temporary Error

An error occurred which is due to a temporary server error.

##### Invalid Arguments

An invalid argument was passed to an SDK method.  This may not be as greatly
used in typed languages (as it is much harder to send invalid arguments),
however I suspect it may still be used occasionally.

##### Key Not Found

Should be sent if an operation fails due to a key not being found on the server.

##### Key Already Exists

Should be sent if an operation fails due to a key already existing on the
server.

## Operations

### Hello

Client -> Server

This operation is performed upon initial connection from a test harness client to the CAVES server.  This operation is used to provide information about the type and capabilities of the connecting client.  Note that no other commands will be acknowledged or sent until a `hello` operation is successful.

Request Options:

- version (integer): The CAVES protocol version
- platform (string): The platform that the SDK harness is operating on
  (ex:  windows, centos, ubuntu, debian, etc...).
- architecture (string): The architecture that the SDK harness is operation on  (ex: x86_64, x86).
- clientVersion (string): The SDK version
- clientBuild (string): A string used to better identify this particular version of an SDK, used only for display purposes.
- supports (list[string]): A list of capabilities that this client supports, see the capabilities section for more information.

Example Request:
```json
{
  "seqno": 1,
  "op": "hello",
  "opts": {
    "version": 1,
    "platform": "macos",
    "architecture": "x86_64",
    "clientVersion": "1.0.0",
    "clientBuild": "1.0.0-master",
    "supports": [
      "couchbase",
      "memcached",
      "ssl",
      "observe"
    ]
  }
}
```

Example Response:
```json
{
  "seqno": 1,
  "result": true
}
```

### Goodbye

Server -> Client

This operation will be sent by the server once all testing has been completed.  It is expected that the stream will be closed following this message, and the test harness is expected to shutdown as soon as possible.

Example Request:
```json
{
  "seqno": 1,
  "op": "goodbye"
}
```

No Response


### Log Message

Client -> Server

This operation will log a message to the servers log stream.  This may be
attached to a specific operation or test, or it may appear only in the global
log, depending on the state of the test runner.

Example Request:
```json
{
  "op": "log",
  "opts": {
    "level": 0,
    "text":
  }
}
```
