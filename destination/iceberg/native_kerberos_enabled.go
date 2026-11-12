//go:build kerberos

package iceberg

// Release builds enable goSASL's real GSSAPI implementation with this tag.
const nativeKerberosAvailable = true
