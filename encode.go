package kdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"reflect"
	"time"
)

// TODO: Handle all the errors returned by `Write` calls
// To read more about Qipc protocol, see https://code.kx.com/wiki/Reference/ipcprotocol
// Negative types are scalar and positive ones are vector. 0 is mixed list
func writeData(dbuf *bytes.Buffer, order binary.ByteOrder, data *K) error {
	binary.Write(dbuf, order, data.Type)

	// For all vector types, write the attribute (s,u,p,g OR none) & length of the vector
	if K0 <= data.Type && data.Type <= KT {
		binary.Write(dbuf, order, data.Attr)
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
	} else if data.Type == XT { // For table only write the attribute
		binary.Write(dbuf, order, data.Attr)
	}

	switch data.Type {
	case K0: // Mixed List
		for _, k := range data.Data.([]*K) {
			if err := writeData(dbuf, order, k); err != nil {
				return err
			}
		}
	case -KB, -UU, -KG, -KH, -KI, -KJ, -KE, -KF, -KC, -KM, -KZ, -KN, -KU, -KV, -KT,
		KB, UU, KG, KH, KI, KJ, KE, KF, KM, KZ, KN, KU, KV, KT: // Bool, Int, Float, and Byte
		// Note: UUID is backed by byte array of length 16
		if err := binary.Write(dbuf, order, data.Data); err != nil {
			log.Println("Error writing", data.Data, err)
		}
	case KC: // String
		dbuf.WriteString(data.Data.(string))
	case -KS: // Symbol
		dbuf.WriteString(data.Data.(string))
		binary.Write(dbuf, order, byte(0)) // Null terminator
	case KS: // Symbol
		for _, symbol := range data.Data.([]string) {
			dbuf.WriteString(symbol)
			binary.Write(dbuf, order, byte(0)) // Null terminator
		}
	case -KP: // Timestamp
		binary.Write(dbuf, order, data.Data.(time.Time).Sub(qEpoch))
	case KP: // Timestamp
		for _, ts := range data.Data.([]time.Time) {
			binary.Write(dbuf, order, ts.Sub(qEpoch))
		}
	case -KD: // Date
		date := data.Data.(time.Time)
		days := (date.Truncate(time.Hour*24).Unix() - qEpoch.Unix()) / 86400
		binary.Write(dbuf, order, int32(days))
	case KD: // Date
		for _, date := range data.Data.([]time.Time) {
			days := (date.Truncate(time.Hour*24).Unix() - qEpoch.Unix()) / 86400
			binary.Write(dbuf, order, int32(days))
		}
	// case -KT: // Time
	// 	t := data.Data.(time.Time)
	// 	nanos := time.Duration(t.Hour())*time.Hour +
	// 		time.Duration(t.Minute())*time.Minute +
	// 		time.Duration(t.Second())*time.Second +
	// 		time.Duration(t.Nanosecond())
	// 	binary.Write(dbuf, order, int32(nanos/time.Millisecond))
	// case KT: // Time
	// 	for _, t := range data.Data.([]time.Time) {
	// 		nanos := time.Duration(t.Hour())*time.Hour +
	// 			time.Duration(t.Minute())*time.Minute +
	// 			time.Duration(t.Second())*time.Second +
	// 			time.Duration(t.Nanosecond())
	// 		binary.Write(dbuf, order, int32(nanos/time.Millisecond))
	// 	}
	case XD: // Dictionary
		dict := data.Data.(Dict)
		err := writeData(dbuf, order, dict.Key)
		if err != nil {
			return err
		}
		err = writeData(dbuf, order, dict.Value)
		if err != nil {
			return err
		}
	case XT: // Table
		table := data.Data.(Table)
		err := writeData(dbuf, order, NewDict(SymbolV(table.Columns), Enlist(table.Data...)))
		if err != nil {
			return err
		}
	case KERR:
		err := data.Data.(error)
		dbuf.WriteString(err.Error())
		binary.Write(dbuf, order, byte(0)) // Null terminator
	case KFUNC:
		fn := data.Data.(Function)
		dbuf.WriteString(fn.Namespace)
		binary.Write(dbuf, order, byte(0)) // Null terminator
		err := writeData(dbuf, order, String(fn.Body))
		if err != nil {
			return err
		}
	case KPROJ, KCOMP:
		d := data.Data.([]*K)
		if err := binary.Write(dbuf, order, int32(len(d))); err != nil {
			return err
		}
		for i := 0; i < len(d); i++ {
			if err := writeData(dbuf, order, d[i]); err != nil {
				return err
			}
		}
	case KEACH, KOVER, KSCAN, KPRIOR, KEACHRIGHT, KEACHLEFT:
		return writeData(dbuf, order, data.Data.(*K))
	case KFUNCUP, KFUNCBP, KFUNCTR:
		b := data.Data.(byte)
		if err := binary.Write(dbuf, order, &b); err != nil {
			return err
		}
	default:
		return NewUnsupportedTypeError(fmt.Sprintf("Unsupported Type: %d", data.Type))
	}
	return nil
}

// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype ReqType, data *K) error {
	var order = binary.LittleEndian
	buf := new(bytes.Buffer)

	// As a place holder header, write 8 bytes to the buffer
	header := [8]byte{}
	if _, err := buf.Write(header[:]); err != nil {
		return err
	}

	// Then write the qipc encoded data
	if err := writeData(buf, order, data); err != nil {
		return err
	}

	// Now that we have the length of the buffer, create the correct header
	header[0] = 1 // byte order
	header[1] = byte(msgtype)
	header[2] = 0
	header[3] = 0
	order.PutUint32(header[4:], uint32(buf.Len()))

	// Write the correct header to the original buffer
	b := buf.Bytes()
	copy(b, header[:])

	_, err := w.Write(Compress(b))
	return err
}
