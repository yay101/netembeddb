package protocol

const (
	Magic = "NEDS"

	OpInsert      = 0x01
	OpGet         = 0x02
	OpUpdate      = 0x03
	OpDelete      = 0x04
	OpQuery       = 0x05
	OpQueryRange  = 0x06
	OpFilter      = 0x07
	OpScan        = 0x08
	OpCount       = 0x09
	OpVacuum      = 0x0A
	OpCreateTable = 0x0B
	OpListTables  = 0x0C
	OpCreateIndex = 0x0D
	OpDropIndex   = 0x0E
	OpQueryPaged  = 0x0F
	OpFilterPaged = 0x10
	OpClose       = 0xFF

	OpResponse = 0x80
	OpError    = 0x81
	OpStream   = 0x82
)

type MessageType byte

func (m MessageType) String() string {
	switch m {
	case OpInsert:
		return "INSERT"
	case OpGet:
		return "GET"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpQuery:
		return "QUERY"
	case OpQueryRange:
		return "QUERY_RANGE"
	case OpFilter:
		return "FILTER"
	case OpScan:
		return "SCAN"
	case OpCount:
		return "COUNT"
	case OpVacuum:
		return "VACUUM"
	case OpCreateTable:
		return "CREATE_TABLE"
	case OpListTables:
		return "LIST_TABLES"
	case OpCreateIndex:
		return "CREATE_INDEX"
	case OpDropIndex:
		return "DROP_INDEX"
	case OpQueryPaged:
		return "QUERY_PAGED"
	case OpFilterPaged:
		return "FILTER_PAGED"
	case OpResponse:
		return "RESPONSE"
	case OpError:
		return "ERROR"
	case OpStream:
		return "STREAM"
	default:
		return "UNKNOWN"
	}
}

type RangeFlags byte

const (
	RangeMinInclusive RangeFlags = 1 << iota
	RangeMaxInclusive
)

type QueryRangeType byte

const (
	QueryRangeGreaterThan QueryRangeType = 0x01
	QueryRangeLessThan    QueryRangeType = 0x02
	QueryRangeBetween     QueryRangeType = 0x03
)

type FilterOperator byte

const (
	FilterOpEqual       FilterOperator = 0x01
	FilterOpNotEqual    FilterOperator = 0x02
	FilterOpGreaterThan FilterOperator = 0x03
	FilterOpLessThan    FilterOperator = 0x04
	FilterOpGreaterOrEq FilterOperator = 0x05
	FilterOpLessOrEq    FilterOperator = 0x06
	FilterOpLike        FilterOperator = 0x07
)
