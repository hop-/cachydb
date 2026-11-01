package config

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    ByteSize
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "1024", want: 1024},
		{in: "10B", want: 10},
		{in: "1KiB", want: 1 << 10},
		{in: "64MiB", want: 64 << 20},
		{in: "1GiB", want: 1 << 30},
		{in: "2TiB", want: 2 << 40},
		{in: "1KB", want: 1000},
		{in: "5MB", want: 5_000_000},
		{in: "1GB", want: 1_000_000_000},
		{in: "1TB", want: 1_000_000_000_000},
		{in: "64mib", want: 64 << 20},
		{in: " 1 GiB ", want: 1 << 30},
		{in: "1.5GiB", want: 3 << 29},
		{in: "0.5KiB", want: 512},
		{in: "", wantErr: true},
		{in: "GiB", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "-1", wantErr: true},
		{in: "-1MiB", wantErr: true},
		{in: "-0.5MiB", wantErr: true},
		{in: "1.5B", wantErr: true},
		{in: "1PiB", wantErr: true},
		{in: "10000000TiB", wantErr: true},
		{in: "9223372036854775807KB", wantErr: true},
		{in: "NaN", wantErr: true},
		{in: "Inf", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseByteSize(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseByteSize(%q) = %d, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseByteSize(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseByteSize(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestByteSizeString(t *testing.T) {
	tests := []struct {
		in   ByteSize
		want string
	}{
		{in: 0, want: "0B"},
		{in: 1, want: "1B"},
		{in: 1000, want: "1000B"},
		{in: KiB, want: "1KiB"},
		{in: 1536, want: "1536B"},
		{in: 64 * MiB, want: "64MiB"},
		{in: 1536 * MiB, want: "1536MiB"},
		{in: GiB, want: "1GiB"},
		{in: 3 * TiB, want: "3TiB"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("ByteSize(%d).String() = %q, want %q", int64(tt.in), got, tt.want)
			}
			back, err := ParseByteSize(tt.in.String())
			if err != nil {
				t.Fatalf("round trip of %q: %v", tt.want, err)
			}
			if back != tt.in {
				t.Fatalf("round trip of %q = %d, want %d", tt.want, back, tt.in)
			}
		})
	}
}

func TestByteSizeSetAndDecode(t *testing.T) {
	tests := []struct {
		name    string
		set     func(b *ByteSize, v string) error
		in      string
		want    ByteSize
		wantErr bool
	}{
		{name: "set", set: (*ByteSize).Set, in: "64MiB", want: 64 * MiB},
		{name: "decode", set: (*ByteSize).Decode, in: "1GiB", want: GiB},
		{name: "set invalid keeps value", set: (*ByteSize).Set, in: "lots", want: 7, wantErr: true},
		{name: "decode invalid keeps value", set: (*ByteSize).Decode, in: "-3KiB", want: 7, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := ByteSize(7)
			err := tt.set(&b, tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if b != tt.want {
				t.Fatalf("value = %d, want %d", b, tt.want)
			}
			if got := b.Type(); got != "size" {
				t.Fatalf("Type() = %q, want %q", got, "size")
			}
		})
	}
}
