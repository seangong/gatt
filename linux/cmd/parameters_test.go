package cmd

import (
	"bytes"
	"testing"
)

func TestLEReadCommandsParameterEncoding(t *testing.T) {
	for _, tt := range []struct {
		name string
		cp   CmdParam
		want []byte
	}{
		{"buffer size", LEReadBufferSize{}, []byte{1, 2, 0x20, 0}},
		{"remote features", LEReadRemoteUsedFeatures{ConnectionHandle: 0x0123}, []byte{1, 0x16, 0x20, 2, 0x23, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			packet := cmdPkt{op: tt.cp.Opcode(), cp: tt.cp}
			if got := packet.Marshal(); !bytes.Equal(got, tt.want) {
				t.Fatalf("command packet = %x, want %x", got, tt.want)
			}
		})
	}
}
