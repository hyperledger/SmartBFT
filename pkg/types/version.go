// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types

// Protocol versions isolate nodes whose consensus protocols are not compatible.
// Version 2 is not compatible with earlier versions, so a node of version 2 accepts no earlier version.
//
// The upgrade path to version 2 is:
//   - nodes from before v1.0.3, which send no version, upgrade to v1.0.3 (version 1) first, which can be a rolling upgrade;
//   - then the cluster moves from version 1 to version 2 with downtime: every node is shut down before any node runs version 2,
//     so version 1 and version 2 nodes never run together.
//
// A node of version 1 ignores the messages of version 2 nodes, and the other way around.
// A node from before v1.0.3 doesn't check the version, and processes the messages of version 2 nodes with the old protocol,
// so it is not safe next to them.
//
// Only consensus messages carry the version. Forwarded client requests and the decisions an application syncs from other nodes
// carry none, so keeping them apart is the application's responsibility.
const (
	// ProtocolVersion is the version of the consensus protocol a node sends in every consensus message.
	ProtocolVersion uint64 = 2

	// MinSupportedProtocolVersion is the lowest protocol version a node accepts messages of.
	// Messages without a version come from nodes that predate versioning, and have version 0.
	// A change to the protocol that is not compatible with earlier versions raises it to the new ProtocolVersion.
	MinSupportedProtocolVersion uint64 = 2
)

// SupportedProtocolVersion returns whether a node accepts messages of the given protocol version.
func SupportedProtocolVersion(version uint64) bool {
	return supportedProtocolVersion(version, MinSupportedProtocolVersion, ProtocolVersion)
}

func supportedProtocolVersion(version, minimum, maximum uint64) bool {
	return minimum <= version && version <= maximum
}
