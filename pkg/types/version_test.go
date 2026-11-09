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
	require.True(t, SupportedProtocolVersion(0), "messages without a version are accepted")
	require.True(t, SupportedProtocolVersion(ProtocolVersion))
	require.False(t, SupportedProtocolVersion(ProtocolVersion+1))

	// once a change raises the minimum, earlier versions are no longer accepted
	require.False(t, supportedProtocolVersion(1, 2, 2))
	require.True(t, supportedProtocolVersion(2, 2, 2))
	require.False(t, supportedProtocolVersion(3, 2, 2))
}
