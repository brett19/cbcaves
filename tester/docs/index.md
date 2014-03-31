# CAVES - Client Analysis, Verification and Experimentation System

CAVES is a platform designed to aid in the development, debugging and
verification of SDK's being developed within Couchbase.  The platform consists
of a number of integrated and external components which together make up CAVES.
A quick description of the purpose and integrations of each of these components
is available below.

Note that CAVES is currently being developed for specific use by the SDK
development team, to enhance productivity when verifying changes or new features
as well as reducing the possibility of regressions occurring.  Additionally, it
was created to allow fast iteration and quick prototyping of new server
features, as a result we currently provide no guarantees regarding the stability
or longevity of the codebase, though we do not expect to ever propagate false
acceptances when performing SDK verifications.

## Components

### Core

The CAVES core provides a Web UI to allow controlling and configuration of
CAVES.  This component is also responsible for accepting connections from the
various client harnesses and initiating the execution of tests against it.

### Test Runner

The test runner is the component which is in charge of executing a multitude of
tests against incoming harness connections.  From these tests, reports are
generated for the clients which describe all issues exposed by the tests.  The
test runner is also able to be executed in a stand-alone fashion along with the
RI client to allow performing 'server tests' to ensure the mock server's
behaviour is accurately replicating a real cluster.

### Mock Server

The mock server implements a full Couchbase cluster, it is capable of handling
all the requests and operations of a real cluster.  In addition to emulating the
behaviour of a Couchbase cluster, the mock additionally provides a wide range of
introspection, statistical and instrumentation capabilities to allow writing
tests which are both accurate and deterministic.

### RI Client

This component provides a well documented and well behaved client.  This
component serves multiple purposes.  The primary of which is to allow for quick
experimentation and iteration on new features which are expected to be later
implemented in all SDK's, this is useful for quickly building verification tests
to be performed against other SDK's.  This client is also used by the CAVES test
runner when performing tests against real clusters (as opposed to the mock) in
which performing 'out of band' operations against the cluster is necessary (for
instance, verifying the a client correctly performed a specific storage
operation).

### SDK Harnesses

The SDK Harness's are responsible for providing a method of controlling each
client SDK from the test runner.  These harness's implement the protocol
specified by CAVES and perform operations against their respective SDKs based on
the requests received via the protocol.

### Mock Server Package

NOT YET AVAILABLE

The mock server harness allows the stand-alone instantiation of a mock cluster
from an application which is not CAVES.  This can be used to allow writing
client-specific unit tests (for instance, for libcouchbase).  As this component
is not yet available, alternatively the 'CAVES Package' may be developed
instead.


### CAVES Package

NOT YET AVAILABLE

This component would represent a packaged version of CAVES with specific tests
which can be downloaded and executed by a specific client to ensure all cluster
related functionality is behaving properly at the end-user side.  These tests
would replace the client-specific cluster-specific unit-tests, allowing the
reuse of CAVES existing high quality tests.
