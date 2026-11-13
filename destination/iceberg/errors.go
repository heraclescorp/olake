//go:build !kerberos

package iceberg

// goSASL's untagged GSSAPI entry point panics; reject SASL configuration before
// entering it rather than silently selecting an unauthenticated transport.
const nativeKerberosAvailable = false
