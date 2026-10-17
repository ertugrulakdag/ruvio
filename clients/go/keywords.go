package ruvio

import "strconv"

// Typed command keywords. Each type is a string type, so an untyped string
// literal such as "km" or "LEFT" still compiles where the type is expected.
// The known values encode through pre-built RESP fragments (no per-call
// allocation); any other value is sent as given and the server decides.

var (
	kwM       = preEnc("m")
	kwKM      = preEnc("km")
	kwFT      = preEnc("ft")
	kwMI      = preEnc("mi")
	kwASC     = preEnc("ASC")
	kwDESC    = preEnc("DESC")
	kwLEFT    = preEnc("LEFT")
	kwRIGHT   = preEnc("RIGHT")
	kwBYTE    = preEnc("BYTE")
	kwAND     = preEnc("AND")
	kwOR      = preEnc("OR")
	kwXOR     = preEnc("XOR")
	kwNOT     = preEnc("NOT")
	kwSUM     = preEnc("SUM")
	kwMIN     = preEnc("MIN")
	kwSET     = preEnc("SET")
	kwINCRBY  = preEnc("INCRBY")
	kwOVERFLW = preEnc("OVERFLOW")
	kwWRAP    = preEnc("WRAP")
	kwSAT     = preEnc("SAT")
	kwFAIL    = preEnc("FAIL")

	kwScanTypes = map[ScanType][]byte{
		ScanString: preEnc("string"), ScanJSON: preEnc("json"), ScanList: preEnc("list"),
		ScanSet: preEnc("set"), ScanHash: preEnc("hash"), ScanZSet: preEnc("zset"),
		ScanBloomFilter: preEnc("bloomfilter"), ScanStream: preEnc("stream"),
		ScanLease: preEnc("lease"), ScanLimit: preEnc("limit"),
		ScanSemaphore: preEnc("semaphore"), ScanOnce: preEnc("once"),
	}
)

// GeoUnit is a GEO distance unit. Valid values: GeoMeters ("m"), GeoKilometers
// ("km"), GeoFeet ("ft"), GeoMiles ("mi"). The zero value means meters.
//
//	d, ok, err := c.GeoDist(ctx, "cities", "a", "b", ruvio.GeoKilometers)
type GeoUnit string

// GEO distance units.
const (
	GeoMeters     GeoUnit = "m"
	GeoKilometers GeoUnit = "km"
	GeoFeet       GeoUnit = "ft"
	GeoMiles      GeoUnit = "mi"
)

func (u GeoUnit) token() []byte {
	switch u {
	case "m", "":
		return kwM
	case "km":
		return kwKM
	case "ft":
		return kwFT
	case "mi":
		return kwMI
	}
	return nil
}

// GeoOrder is a GEO result order. Valid values: GeoAsc ("ASC"), GeoDesc ("DESC");
// the zero value leaves the order unspecified.
//
//	q := ruvio.GeoSearchQuery{Radius: 5, Unit: ruvio.GeoKilometers, Sort: ruvio.GeoAsc}
type GeoOrder string

// GEO result orders.
const (
	GeoAsc  GeoOrder = "ASC"
	GeoDesc GeoOrder = "DESC"
)

func (o GeoOrder) token() []byte {
	switch o {
	case "ASC":
		return kwASC
	case "DESC":
		return kwDESC
	}
	return nil
}

// ListSide names the end of a list: ListLeft ("LEFT") or ListRight ("RIGHT").
// Used by LMove, BLMove, LMPop and BLMPop.
//
//	v, ok, err := c.LMove(ctx, "src", "dst", ruvio.ListLeft, ruvio.ListRight)
type ListSide string

// List ends.
const (
	ListLeft  ListSide = "LEFT"
	ListRight ListSide = "RIGHT"
)

func (s ListSide) token() []byte {
	switch s {
	case "LEFT":
		return kwLEFT
	case "RIGHT":
		return kwRIGHT
	}
	return nil
}

// ListPosition is the LINSERT position: InsertBefore ("BEFORE") or InsertAfter ("AFTER").
//
//	n, err := c.LInsertAt(ctx, "k", ruvio.InsertBefore, "pivot", "value")
type ListPosition string

// LINSERT positions.
const (
	InsertBefore ListPosition = "BEFORE"
	InsertAfter  ListPosition = "AFTER"
)

func (p ListPosition) token() []byte {
	switch p {
	case "BEFORE":
		return tBEFORE
	case "AFTER":
		return tAFTER
	}
	return nil
}

// ScanType filters SCAN by key type. Valid values: ScanString ("string"), ScanJSON
// ("json"), ScanList ("list"), ScanSet ("set"), ScanHash ("hash"), ScanZSet ("zset"),
// ScanBloomFilter ("bloomfilter"), ScanStream ("stream"), ScanLease ("lease"),
// ScanLimit ("limit"), ScanSemaphore ("semaphore"), ScanOnce ("once").
//
//	c.Scan(ctx, "0", ruvio.ScanOptions{Type: ruvio.ScanHash})
type ScanType string

// SCAN key types.
const (
	ScanString      ScanType = "string"
	ScanJSON        ScanType = "json"
	ScanList        ScanType = "list"
	ScanSet         ScanType = "set"
	ScanHash        ScanType = "hash"
	ScanZSet        ScanType = "zset"
	ScanBloomFilter ScanType = "bloomfilter"
	ScanStream      ScanType = "stream"
	ScanLease       ScanType = "lease"
	ScanLimit       ScanType = "limit"
	ScanSemaphore   ScanType = "semaphore"
	ScanOnce        ScanType = "once"
)

func (t ScanType) token() []byte { return kwScanTypes[t] }

// BitOperation is the BITOP operation: BitAnd, BitOr, BitXor or BitNot.
//
//	n, err := c.BitOp(ctx, ruvio.BitAnd, "dest", "a", "b")
type BitOperation string

// BITOP operations.
const (
	BitAnd BitOperation = "AND"
	BitOr  BitOperation = "OR"
	BitXor BitOperation = "XOR"
	BitNot BitOperation = "NOT"
)

func (o BitOperation) token() []byte {
	switch o {
	case "AND":
		return kwAND
	case "OR":
		return kwOR
	case "XOR":
		return kwXOR
	case "NOT":
		return kwNOT
	}
	return nil
}

// BitUnit is the BITCOUNT range unit: BitUnitByte ("BYTE") or BitUnitBit ("BIT").
//
//	n, err := c.BitCountUnit(ctx, "k", 0, 15, ruvio.BitUnitBit)
type BitUnit string

// BITCOUNT range units.
const (
	BitUnitByte BitUnit = "BYTE"
	BitUnitBit  BitUnit = "BIT"
)

func (u BitUnit) token() []byte {
	switch u {
	case "BYTE":
		return kwBYTE
	case "BIT":
		return tBIT
	}
	return nil
}

// Aggregate is the ZUNION/ZINTER score combiner: AggSum, AggMin or AggMax; the
// zero value leaves the server default (SUM).
//
//	z := ruvio.ZStore{Keys: []string{"a", "b"}, Aggregate: ruvio.AggMax}
type Aggregate string

// Score aggregation modes.
const (
	AggSum Aggregate = "SUM"
	AggMin Aggregate = "MIN"
	AggMax Aggregate = "MAX"
)

func (a Aggregate) token() []byte {
	switch a {
	case "SUM":
		return kwSUM
	case "MIN":
		return kwMIN
	case "MAX":
		return tMAX
	}
	return nil
}

// BitFieldOpName is a BITFIELD sub-command: BitFieldGet, BitFieldSet,
// BitFieldIncrBy or BitFieldOverflow (lower case is accepted too).
//
//	ruvio.BitFieldOp{Op: ruvio.BitFieldSet, Type: ruvio.BitU8, Offset: "0", Value: 7}
type BitFieldOpName string

// BITFIELD sub-commands.
const (
	BitFieldGet      BitFieldOpName = "GET"
	BitFieldSet      BitFieldOpName = "SET"
	BitFieldIncrBy   BitFieldOpName = "INCRBY"
	BitFieldOverflow BitFieldOpName = "OVERFLOW"
)

func (o BitFieldOpName) token() []byte {
	switch o {
	case "GET", "get":
		return tGET
	case "SET", "set":
		return kwSET
	case "INCRBY", "incrby":
		return kwINCRBY
	case "OVERFLOW", "overflow":
		return kwOVERFLW
	}
	return nil
}

// BITFIELD OVERFLOW modes, used as BitFieldOp.Type when Op is BitFieldOverflow.
const (
	BitOverflowWrap = "WRAP"
	BitOverflowSat  = "SAT"
	BitOverflowFail = "FAIL"
)

func overflowToken(s string) []byte {
	switch s {
	case "WRAP":
		return kwWRAP
	case "SAT":
		return kwSAT
	case "FAIL":
		return kwFAIL
	}
	return nil
}

// Common BITFIELD field types.
const (
	BitI8  = "i8"
	BitI16 = "i16"
	BitI32 = "i32"
	BitI64 = "i64"
	BitU8  = "u8"
	BitU16 = "u16"
	BitU32 = "u32"
)

// BitSigned returns the BITFIELD type for a signed field of the given width (1..64).
//
//	ruvio.BitFieldOp{Op: ruvio.BitFieldGet, Type: ruvio.BitSigned(5), Offset: "#1"}
func BitSigned(bits int) string { return "i" + strconv.Itoa(bits) }

// BitUnsigned returns the BITFIELD type for an unsigned field of the given width (1..63).
func BitUnsigned(bits int) string { return "u" + strconv.Itoa(bits) }

// kw appends tok when known, else the raw value.
func (r *request) kw(tok []byte, raw string) *request {
	if tok != nil {
		return r.t(tok)
	}
	return r.s(raw)
}
