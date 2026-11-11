// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSupportedProtocolVersion(t *testing.T) {
	require.False(t, SupportedProtocolVersion(0), "messages without a version are not accepted")
	require.False(t, SupportedProtocolVersion(1), "messages of v1.0.3 are not accepted")
	require.True(t, SupportedProtocolVersion(2))
	require.False(t, SupportedProtocolVersion(ProtocolVersion+1))

	// a range of versions
	require.False(t, supportedProtocolVersion(1, 2, 3))
	require.True(t, supportedProtocolVersion(2, 2, 3))
	require.True(t, supportedProtocolVersion(3, 2, 3))
	require.False(t, supportedProtocolVersion(4, 2, 3))

	// a single version
	require.False(t, supportedProtocolVersion(2, 3, 3))
	require.True(t, supportedProtocolVersion(3, 3, 3))
	require.False(t, supportedProtocolVersion(4, 3, 3))
}
