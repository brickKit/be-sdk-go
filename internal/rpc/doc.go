// Package rpc is the system plane of be-protocol P7: the gRPC server every member runs on its grpc
// port (interceptors, limits, error normalisation) and the outbound connections a member keeps to its
// dependencies (connection reuse, deadlines, retries by contract, bulkhead, metadata, transaction
// guard). Everything is per instance: a shell builds one Server and one Conns per member (P19.4).
package rpc
