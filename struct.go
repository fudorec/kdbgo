package kdb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// ReqType represents type of message sent or recieved via ipc
type ReqType int8

// Constants for recognised request types
const (
	ASYNC ReqType = iota
	SYNC
	RESPONSE
)

// Attr denotes attribute set on a non-scalar object
type Attr int8

// Constants for recognised attributes
const (
	NONE Attr = iota
	SORTED
	UNIQUE
	PARTED
	GROUPED
)

// Q type constants
const (
	K0 int8 = 0 // generic type
	//      type bytes qtype     ctype
	KB int8 = 1  // 1 boolean   char
	UU int8 = 2  // 16 guid     U
	KG int8 = 4  // 1 byte      char
	KH int8 = 5  // 2 short     short
	KI int8 = 6  // 4 int       int
	KJ int8 = 7  // 8 long      long
	KE int8 = 8  // 4 real      float
	KF int8 = 9  // 8 float     double
	KC int8 = 10 // 1 char      char
	KS int8 = 11 // * symbol    char*

	KP int8 = 12 // 8 timestamp long    nanoseconds from 2000.01.01
	KM int8 = 13 // 4 month     int     months from 2000.01.01
	KD int8 = 14 // 4 date      int     days from 2000.01.01
	KZ int8 = 15 // 8 datetime  double  deprecated - DO NOT USE
	KN int8 = 16 // 8 timespan  long    nanoseconds
	KU int8 = 17 // 4 minute    int
	KV int8 = 18 // 4 second    int
	KT int8 = 19 // 4 time      int     millisecond

	// table,dict
	XT int8 = 98  //   pointer to dictionary containing string keys(column names) and values
	XD int8 = 99  //   2 element generic list with 0 as keys and 1 as values
	SD int8 = 127 // 	 sorted dict - acts as a step function

	// function types
	KFUNC      int8 = 100
	KFUNCUP    int8 = 101 // unary primitive
	KFUNCBP    int8 = 102 // binary primitive
	KFUNCTR    int8 = 103 // ternary (operator)
	KPROJ      int8 = 104 // projection
	KCOMP      int8 = 105 // composition
	KEACH      int8 = 106 // f'
	KOVER      int8 = 107 //	f/
	KSCAN      int8 = 108 // f\
	KPRIOR     int8 = 109 // f':
	KEACHRIGHT int8 = 110 // f/:
	KEACHLEFT  int8 = 111 // f\:
	KDYNLOAD   int8 = 112 // dynamic loaded libraries - not available in IPC

	// error type
	KERR int8 = -128 // indicates error with 0 terminated string as a text
)

// The Qipc header follows the contract below
// 0x00         - 1 Byte 	- Architecture used for encoding the message, BigEndian (0) or LittleEndian (1)
// 0x00         - 1 Byte 	- Message type,  Async (0) or Sync (1) or response (2)
// 0x0000       - 2 Bytes 	- Compressed & Reserved flag
// 0x00000000   - 4 Bytes	- Message length
type ipcHeader struct {
	ByteOrder   byte
	RequestType ReqType
	Compressed  byte
	Reserved    byte
	MsgSize     uint32
}

// Nh is a short nil
const Nh int16 = math.MinInt16

// Wh is a short infinity
const Wh int16 = math.MaxInt16

// Ni is an int nil
const Ni int32 = math.MinInt32

// Wi is an int infinity
const Wi int32 = math.MaxInt32

// Nj is a long nil
const Nj int64 = math.MinInt64

// Wj is a long infinity
const Wj int64 = math.MaxInt64

// Ne is a real nil
var Ne = float32(math.NaN())

// We is a real infinity
var We = float32(math.Inf(+1))

// Nf is a double nil
var Nf = math.NaN()

// Wf is a double infinity
var Wf = math.Inf(+1)

// K structure
type K struct {
	Type int8
	Attr Attr
	Data interface{}
}

func Bool(x bool) *K {
	return &K{Type: -KB, Attr: NONE, Data: x}
}

func BoolV(x []bool) *K {
	return &K{Type: KB, Attr: NONE, Data: x}
}

func Guid(x uuid.UUID) *K {
	return &K{Type: -UU, Attr: NONE, Data: x}
}

func GuidV(x []uuid.UUID) *K {
	return &K{Type: UU, Attr: NONE, Data: x}
}

var UUID = Guid
var UUIDV = GuidV

func Byte(x byte) *K {
	return &K{Type: -KG, Attr: NONE, Data: x}
}

func ByteV(x []byte) *K {
	return &K{Type: KG, Attr: NONE, Data: x}
}

func Short(x int16) *K {
	return &K{Type: -KH, Attr: NONE, Data: x}
}

func ShortV(x []int16) *K {
	return &K{Type: KH, Attr: NONE, Data: x}
}

var K0Nh = Short(Nh)

// Int wraps int32 as K
func Int(x int32) *K {
	return &K{-KI, NONE, x}
}

// IntV wraps int32 slice as K
func IntV(x []int32) *K {
	return &K{KI, NONE, x}
}

var K0Ni = Int(Ni)

// Long wraps int64 as K
func Long(x int64) *K {
	return &K{-KJ, NONE, x}
}

// LongV wraps int64 slice as K
func LongV(x []int64) *K {
	return &K{KJ, NONE, x}
}

var K0Nj = Long(Nj)

// Real wraps float32 as K
func Real(x float32) *K {
	return &K{-KE, NONE, x}
}

// RealV wraps float32 slice as K
func RealV(x []float32) *K {
	return &K{KE, NONE, x}
}

var K0Ne = Real(Ne)

// Float wraps float64 as K
func Float(x float64) *K {
	return &K{-KF, NONE, x}
}

// FloatV wraps float64 as K
func FloatV(x []float64) *K {
	return &K{KF, NONE, x}
}

var K0Nf = Float(Nf)

func String(x string) *K {
	return &K{Type: KC, Attr: NONE, Data: x}
}

// Symbol wraps string as K
func Symbol(x string) *K {
	return &K{-KS, NONE, x}
}

// SymbolV wraps string slice as K
func SymbolV(x []string) *K {
	return &K{KS, NONE, x}
}

var K0Ns = Symbol("")

func Timestamp(x time.Time) *K {
	return &K{Type: -KP, Attr: NONE, Data: x.UTC()}
}

func TimestampV(x []time.Time) *K {
	for i, t := range x {
		x[i] = t.UTC()
	}
	return &K{Type: KP, Attr: NONE, Data: x}
}

var K0Np = Timestamp(time.Unix(0, Nj))

func Date(x time.Time) *K {
	return &K{Type: -KD, Attr: NONE, Data: x.UTC()}
}

func DateV(x []time.Time) *K {
	for i, t := range x {
		x[i] = t.UTC()
	}
	return &K{Type: KD, Attr: NONE, Data: x}
}

var K0Nd = Timestamp(time.Unix(0, Nj))

// func Time(x time.Time) *K {
// 	return &K{Type: -KT, Attr: NONE, Data: x.UTC()}
// }

//	func TimeV(x []time.Time) *K {
//		for i, t := range x {
//			x[i] = t.UTC()
//		}
//		return &K{Type: KT, Attr: NONE, Data: x}
//	}

func Time(x int32) *K {
	return &K{Type: -KT, Attr: NONE, Data: x}
}

func TimeV(x []int32) *K {
	return &K{Type: KT, Attr: NONE, Data: x}
}

var K0Nt = Time(Ni)

func Timespan(x time.Duration) *K {
	return &K{Type: -KN, Attr: NONE, Data: x}
}

func TimespanV(x []time.Duration) *K {
	return &K{Type: KN, Attr: NONE, Data: x}
}

var K0Nn = &K{Type: -KN, Attr: NONE, Data: Nj}

// Atom constructs generic K atom with given type
func Atom(t int8, x interface{}) *K {
	return &K{Type: t, Attr: NONE, Data: x}
}

// Error constructs K error object from Go error
func Error(x error) *K {
	return &K{Type: KERR, Attr: NONE, Data: x}
}

// NewList constructs generic list(type 0) from list of K arguments
func NewList(x ...*K) *K {
	return &K{Type: K0, Attr: NONE, Data: x}
}

var Enlist = NewList

func NewTable(cols []string, data ...*K) *K {
	return &K{Type: XT, Attr: NONE, Data: Table{cols, data}}
}

func NewDict(k, v *K) *K {
	return &K{Type: XD, Attr: NONE, Data: Dict{k, v}}
}

// NewFunc creates K function with body in ctx namespace
func NewFunc(ctx, body string) *K {
	return &K{Type: KFUNC, Attr: NONE, Data: Function{Namespace: ctx, Body: body}}
}

// Len returns number of elements in K structure
// Special cases:
// Atoms and functions = 1
// Dictionaries = number of keys
// Tables = number of rows
func (k *K) Len() int {
	if k.Type < K0 || k.Type >= KFUNC {
		return 1
	} else if k.Type >= K0 && k.Type <= KT {
		return reflect.ValueOf(k.Data).Len()
	} else if k.Type == XD {
		return k.Data.(Dict).Key.Len()
	} else if k.Type == XT {
		return k.Data.(Table).Data[0].Len()
	} else {
		return -1
	}
}

// Index returns i'th element of K structure
func (k *K) Index(i int) interface{} {
	if k.Type < K0 || k.Type > XT {
		return nil
	}
	if k.Len() == 0 {
		// need to return null of that type
		if k.Type == K0 {
			return &K{K0, NONE, make([]*K, 0)}
		}
		return nil

	}
	if k.Type >= K0 && k.Type <= KT {
		return reflect.ValueOf(k.Data).Index(i).Interface()
	}
	// case for table
	// need to return dict with header
	if k.Type != XT {
		return nil
	}
	var t = k.Data.(Table)
	return &K{XD, NONE, t.Index(i)}
}

// https://github.com/CharlesSkelton/studio/blob/master/src/studio/kdb/K.java
var attrPrint = []string{NONE: "", SORTED: "`s#", UNIQUE: "`u#", PARTED: "`p#", GROUPED: "`g#"}
var unaryops = []string{"::", "+:", "-:", "*:", "%:", "&:", "|:", "^:", "=:", "<:", ">:", "$:", ",:", "#:", "_:", "~:", "!:", "?:", "@:", ".:", "0::", "1::", "2::", "avg", "last", "sum", "prd", "min", "max", "exit", "getenv", "abs", "sqrt", "log", "exp", "sin", "asin", "cos", "acos", "tan", "atan", "enlist", 255: ""}
var binaryops = []string{":", "+", "-", "*", "%", "&", "|", "^", "=", "<", ">", "$", ",", "#", "_", "~", "!", "?", "@", ".", "0:", "1:", "2:", "in", "within", "like", "bin", "ss", "insert", "wsum", "wavg", "div", "xexp", "setenv"}
var ternaryops = []string{"'", "/", "\\"}
var adverbs = []string{106: "'", 107: "/", 108: "\\", 109: "':", 110: "/:", 111: "\\:"}

// String converts K structure to string
func (k K) String() string {
	if k.Type < K0 {
		return fmt.Sprint(k.Data)
	}
	if k.Type > K0 && k.Type <= KT {
		return fmt.Sprint(attrPrint[k.Attr], k.Data)
	}
	switch k.Type {
	case K0:
		list := k.Data.([]*K)
		var buf bytes.Buffer
		buf.WriteString(attrPrint[k.Attr])
		buf.WriteString("(")
		for i, l := range list {
			buf.WriteString(l.String())
			if i < len(list)-1 {
				buf.WriteString(";")
			}
		}
		buf.WriteString(")")
		return buf.String()
	case XD:
		return attrPrint[k.Attr] + k.Data.(Dict).String()
	case XT:
		return attrPrint[k.Attr] + k.Data.(Table).String()
	case KFUNC:
		return k.Data.(Function).Body
	case KFUNCUP:
		return unaryops[k.Data.(byte)]
	case KFUNCBP:
		return binaryops[k.Data.(byte)]
	case KFUNCTR:
		return ternaryops[k.Data.(byte)]
	case KPROJ, KCOMP:
		list := k.Data.([]*K)
		var buf bytes.Buffer
		for _, l := range list {
			buf.WriteString(l.String())
		}
		return buf.String()
	case KEACH, KOVER, KSCAN, KPRIOR, KEACHRIGHT, KEACHLEFT:
		return k.Data.(*K).String() + adverbs[k.Type]
	default:
		return "unknown"
	}
}

// ErrBadHeader to indicate invalid header
var ErrBadHeader = errors.New("bad header")

// ErrSyncRequest cannot process sync requests
var ErrSyncRequest = errors.New("nosyncrequest")

// NewUnsupportedTypeError initializes a new UnsupportedTypeError with a message.
func NewUnsupportedTypeError(msg string) error {
	return &UnsupportedTypeError{msg}
}

// UnsupportedTypeError is an error returned when encoding or decoding a type that is not supported by kdb
type UnsupportedTypeError struct {
	msg string
}

func (e *UnsupportedTypeError) Error() string {
	return e.msg
}

// NewRuntimeError initializes a new RuntimeError with a message.
func NewRuntimeError(msg string) error {
	return &RuntimeError{msg}
}

// RuntimeError is a runtime error thrown by a Q engine
type RuntimeError struct {
	msg string
}

func (e *RuntimeError) Error() string {
	return e.msg
}

// Epoch offset for Q time. Q epoch starts on 1st Jan 2000
var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// Month represents a month type in kdb
type Month int32

func (m Month) String() string {
	return fmt.Sprintf("%v.%02vm", 2000+int(m/12), int(m)%12)
}

// Minute represents a minute type in kdb
type Minute time.Time

func (m Minute) String() string {
	timeVal := time.Time(m)
	return fmt.Sprintf("%02v:%02v", timeVal.Hour(), timeVal.Minute())

}

// Second represents a second type in kdb - hh:mm:ss
type Second time.Time

func (s Second) String() string {
	timeVal := time.Time(s)
	return fmt.Sprintf("%02v:%02v:%02v", timeVal.Hour(), timeVal.Minute(), timeVal.Second())
}

// Table represents table type in kdb
type Table struct {
	Columns []string
	Data    []*K
}

// Index returns i'th row of the table
func (tbl *Table) Index(i int) Dict {
	var d = Dict{}
	d.Key = &K{KS, NONE, tbl.Columns}
	vslice := make([]*K, len(tbl.Columns))
	d.Value = &K{K0, NONE, vslice}
	for ci := range tbl.Columns {
		kd := tbl.Data[ci].Index(i)
		dtype := tbl.Data[ci].Type
		if dtype == K0 {
			dtype = kd.(*K).Type
		} else if dtype > K0 && dtype <= KT {
			dtype = -dtype
		}
		vslice[ci] = &K{dtype, NONE, kd}
	}
	return d
}

// String prints table
func (tbl Table) String() string {
	var buf bytes.Buffer
	buf.WriteString("+")
	buf.WriteString(fmt.Sprint(tbl.Columns))
	buf.WriteString("!")
	buf.WriteString("(")
	for i, l := range tbl.Data {
		buf.WriteString(l.String())
		if i < len(tbl.Data)-1 {
			buf.WriteString(";")
		}
	}
	buf.WriteString(")")
	return buf.String()

}

// Dict represents ordered key->value mapping.
// Key and Value must be slices of the same length
type Dict struct {
	Key   *K
	Value *K
}

// String
func (d Dict) String() string {
	return fmt.Sprintf("%v!%v", d.Key.Data, d.Value.Data)
}

// titleInitial is utility function to titlecase first letter of the string
func titleInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToTitle(v)) + str[i+1:]
	}
	return ""
}

// UnmarshalDict decodes dict to a struct
func UnmarshalDict(t Dict, v interface{}) error {
	var keys = t.Key.Data.([]string)
	var vals = t.Value.Data.([]*K)
	vv := reflect.ValueOf(v)
	if vv.Kind() != reflect.Ptr || vv.IsNil() {
		return errors.New("invalid target type. Should be non null pointer")
	}
	vv = reflect.Indirect(vv)
	for i := range keys {
		val := vals[i].Data
		fv := vv.FieldByName(titleInitial(keys[i]))
		if !fv.IsValid() {
			continue
		}
		if fv.CanSet() && reflect.TypeOf(val).AssignableTo(fv.Type()) {
			fv.Set(reflect.ValueOf(val))
		}
	}
	return nil
}

// UnmarshalDictToMap decodes dict into map[string]interface{}
func UnmarshalDictToMap(t Dict, v interface{}) error {
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.Map {
		// check if keys are
		kt := vv.Type()
		if kt.Key().Kind() != reflect.String {
			return errors.New("target type should be map[string]T")
		}

		if vv.IsNil() {
			vv.Set(reflect.MakeMap(kt))
		}
	} else {
		return errors.New("target type should be map[string]T")
	}

	if t.Key == nil || t.Value == nil {
		return nil //nothing to decode
	}

	var keys = t.Key.Data.([]string)
	var vals = t.Value.Data.([]*K)

	for i := range keys {
		val := reflect.ValueOf(vals[i].Data)
		kv := reflect.ValueOf(titleInitial(keys[i]))
		vv.SetMapIndex(kv, val)
	}

	return nil
}

// UnmarshalTable decodes table to array of structs
func UnmarshalTable(t Table, v interface{}) (interface{}, error) {
	vv := reflect.ValueOf(v)
	if vv.Kind() != reflect.Ptr || vv.IsNil() {
		return nil, errors.New("invalid target type. shoult be non null pointer")
	}
	vv = reflect.Indirect(vv)
	for i := 0; i < t.Data[0].Len(); i++ {
		emptyelem := reflect.New(vv.Type().Elem())
		err := UnmarshalDict(t.Index(i), emptyelem.Interface())
		if err != nil {
			fmt.Println("Failed to unmrshall dict", err)
			return nil, err
		}
		vv = reflect.Append(vv, reflect.Indirect(emptyelem))
	}
	return vv.Interface(), nil
}

// Function represents function in kdb+
type Function struct {
	Namespace string
	Body      string
}
