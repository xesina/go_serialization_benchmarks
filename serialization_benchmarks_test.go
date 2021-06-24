package goserbench

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	jsoniter "github.com/json-iterator/go"
	"github.com/pierrec/lz4"
)

var (
	validate     = os.Getenv("VALIDATE")
	jsoniterFast = jsoniter.ConfigFastest
)

func randString(l int) string {
	buf := make([]byte, l)
	for i := 0; i < (l+1)/2; i++ {
		buf[i] = byte(rand.Intn(256))
	}
	return fmt.Sprintf("%x", buf)[:l]
}

func generate() []*A {
	a := make([]*A, 0, 1000)
	for i := 0; i < 1000; i++ {
		a = append(a, &A{
			Name:     randString(16),
			BirthDay: time.Now(),
			Phone:    randString(10),
			Siblings: rand.Intn(5),
			Spouse:   rand.Intn(2) == 1,
			Money:    rand.Float64(),
		})
	}
	return a
}

type Serializer interface {
	Marshal(o interface{}) ([]byte, error)
	Unmarshal(d []byte, o interface{}) error
}

func benchMarshal(b *testing.B, s Serializer) {
	b.Helper()
	data := generate()
	b.ReportAllocs()
	b.ResetTimer()
	var serialSize int
	for i := 0; i < b.N; i++ {
		o := data[rand.Intn(len(data))]
		bytes, err := s.Marshal(o)
		if err != nil {
			b.Fatalf("marshal error %s for %#v", err, o)
		}
		serialSize += len(bytes)
	}
	b.ReportMetric(float64(serialSize)/float64(b.N), "B/serial")
}

func cmpTags(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func cmpAliases(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

func benchUnmarshal(b *testing.B, s Serializer) {
	b.Helper()
	b.StopTimer()
	data := generate()
	ser := make([][]byte, len(data))
	var serialSize int
	for i, d := range data {
		o, err := s.Marshal(d)
		if err != nil {
			b.Fatal(err)
		}
		t := make([]byte, len(o))
		serialSize += copy(t, o)
		ser[i] = t
	}
	b.ReportMetric(float64(serialSize)/float64(len(data)), "B/serial")
	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		n := rand.Intn(len(ser))
		o := &A{}
		err := s.Unmarshal(ser[n], o)
		if err != nil {
			b.Fatalf("unmarshal error %s for %#x / %q", err, ser[n], ser[n])
		}
		// Validate unmarshalled data.
		if validate != "" {
			i := data[n]
			correct := o.Name == i.Name && o.Phone == i.Phone && o.Siblings == i.Siblings && o.Spouse == i.Spouse && o.Money == i.Money && o.BirthDay.Equal(i.BirthDay) //&& cmpTags(o.Tags, i.Tags) && cmpAliases(o.Aliases, i.Aliases)
			if !correct {
				b.Fatalf("unmarshaled object differed:\n%v\n%v", i, o)
			}
		}
	}
}

func TestMessage(t *testing.T) {
	println(`
A test suite for benchmarking various Go serialization methods.

See README.md for details on running the benchmarks.
`)
}

// gogo/protobuf

func generateGogoProto() []*GogoProtoBufA {
	a := make([]*GogoProtoBufA, 0, 1000)
	for i := 0; i < 1000; i++ {
		a = append(a, &GogoProtoBufA{
			Name:     randString(16),
			BirthDay: time.Now().UnixNano(),
			Phone:    randString(10),
			Siblings: rand.Int31n(5),
			Spouse:   rand.Intn(2) == 1,
			Money:    rand.Float64(),
		})
	}
	return a
}

func Benchmark_Gogoprotobuf_Marshal(b *testing.B) {
	data := generateGogoProto()
	b.ReportAllocs()
	b.ResetTimer()
	var serialSize int
	for i := 0; i < b.N; i++ {
		bytes, err := proto.Marshal(data[rand.Intn(len(data))])
		if err != nil {
			b.Fatal(err)
		}
		serialSize += len(bytes)
	}
	b.ReportMetric(float64(serialSize)/float64(b.N), "B/serial")
}

func Benchmark_Gogoprotobuf_Unmarshal(b *testing.B) {
	b.StopTimer()
	data := generateGogoProto()
	ser := make([][]byte, len(data))
	var serialSize int
	for i, d := range data {
		var err error
		ser[i], err = proto.Marshal(d)
		if err != nil {
			b.Fatal(err)
		}
		serialSize += len(ser[i])
	}
	b.ReportMetric(float64(serialSize)/float64(len(data)), "B/serial")
	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		n := rand.Intn(len(ser))
		o := &GogoProtoBufA{}
		err := proto.Unmarshal(ser[n], o)
		if err != nil {
			b.Fatalf("goprotobuf failed to unmarshal: %s (%s)", err, ser[n])
		}
		// Validate unmarshalled data.
		if validate != "" {
			i := data[n]
			correct := o.Name == i.Name && o.Phone == i.Phone && o.Siblings == i.Siblings && o.Spouse == i.Spouse && o.Money == i.Money && o.BirthDay == i.BirthDay //&& cmpTags(o.Tags, i.Tags) && cmpAliases(o.Aliases, i.Aliases)
			if !correct {
				b.Fatalf("unmarshaled object differed:\n%v\n%v", i, o)
			}
		}
	}
}

// gogo/protobuf + Snappy

func Benchmark_Gogoprotobuf_Snappy_Marshal(b *testing.B) {
	data := generateGogoProto()
	b.ReportAllocs()
	b.ResetTimer()
	var serialSize int
	for i := 0; i < b.N; i++ {
		bytes, err := proto.Marshal(data[rand.Intn(len(data))])
		if err != nil {
			b.Fatal(err)
		}
		bytes = snappy.Encode(nil, bytes)
		serialSize += len(bytes)
	}
	b.ReportMetric(float64(serialSize)/float64(b.N), "B/serial")
}

func Benchmark_Gogoprotobuf_Snappy_Unmarshal(b *testing.B) {
	b.StopTimer()
	data := generateGogoProto()
	ser := make([][]byte, len(data))
	var serialSize int
	for i, d := range data {
		var err error
		bb, err := proto.Marshal(d)
		if err != nil {
			b.Fatal(err)
		}
		ser[i] = snappy.Encode(nil, bb)
		serialSize += len(ser[i])
	}
	b.ReportMetric(float64(serialSize)/float64(len(data)), "B/serial")
	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		n := rand.Intn(len(ser))
		o := &GogoProtoBufA{}
		bb, err := snappy.Decode(nil, ser[n])
		if err != nil {
			log.Fatal(err)
		}
		err = proto.Unmarshal(bb, o)
		if err != nil {
			b.Fatalf("goprotobuf failed to unmarshal: %s (%s)", err, ser[n])
		}

		// Validate unmarshalled data.
		if validate != "" {
			i := data[n]
			correct := o.Name == i.Name && o.Phone == i.Phone && o.Siblings == i.Siblings && o.Spouse == i.Spouse && o.Money == i.Money && o.BirthDay == i.BirthDay //&& cmpTags(o.Tags, i.Tags) && cmpAliases(o.Aliases, i.Aliases)
			if !correct {
				b.Fatalf("unmarshaled object differed:\n%v\n%v", i, o)
			}
		}
	}
}


// gogo/protobuf + LZ4

func Benchmark_Gogoprotobuf_LZ4_Marshal(b *testing.B) {
	data := generateGogoProto()
	b.ReportAllocs()
	b.ResetTimer()
	var serialSize int
	for i := 0; i < b.N; i++ {
		bytes, err := proto.Marshal(data[rand.Intn(len(data))])
		if err != nil {
			b.Fatal(err)
		}
		compressed := make([]byte, len(bytes))
		_, err = lz4.CompressBlock(bytes, compressed, nil)
		if err != nil {
			b.Fatal(err)
		}
		serialSize += len(bytes)
	}
	b.ReportMetric(float64(serialSize)/float64(b.N), "B/serial")
}

// func Benchmark_Gogoprotobuf_LZ4_Unmarshal(b *testing.B) {
// 	b.StopTimer()
// 	data := generateGogoProto()
// 	ser := make([][]byte, len(data))
// 	var serialSize int
// 	for i, d := range data {
// 		var err error
// 		bb, err := proto.Marshal(d)
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 		ser[i] = snappy.Encode(nil, bb)
// 		serialSize += len(ser[i])
// 	}
// 	b.ReportMetric(float64(serialSize)/float64(len(data)), "B/serial")
// 	b.ReportAllocs()
// 	b.StartTimer()

// 	for i := 0; i < b.N; i++ {
// 		n := rand.Intn(len(ser))
// 		o := &GogoProtoBufA{}
// 		bb, err := snappy.Decode(nil, ser[n])
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		err = proto.Unmarshal(bb, o)
// 		if err != nil {
// 			b.Fatalf("goprotobuf failed to unmarshal: %s (%s)", err, ser[n])
// 		}

// 		// Validate unmarshalled data.
// 		if validate != "" {
// 			i := data[n]
// 			correct := o.Name == i.Name && o.Phone == i.Phone && o.Siblings == i.Siblings && o.Spouse == i.Spouse && o.Money == i.Money && o.BirthDay == i.BirthDay //&& cmpTags(o.Tags, i.Tags) && cmpAliases(o.Aliases, i.Aliases)
// 			if !correct {
// 				b.Fatalf("unmarshaled object differed:\n%v\n%v", i, o)
// 			}
// 		}
// 	}
// }
