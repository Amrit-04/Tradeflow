package fix

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const SOH = '\x01'

// Common FIX Tag definitions
const (
	TagAccount        = 1
	TagAvgPx          = 6
	TagBeginString    = 8
	TagBodyLength     = 9
	TagChecksum       = 10
	TagClOrdID        = 11
	TagCumQty         = 14
	TagExecID         = 17
	TagMsgSeqNum      = 34
	TagMsgType        = 35
	TagOrderID        = 37
	TagOrderQty       = 38
	TagOrdStatus      = 39
	TagPrice          = 44
	TagSenderCompID   = 49
	TagSendingTime    = 52
	TagSide           = 54
	TagSymbol         = 55
	TagTargetCompID   = 56
	TagText           = 58
	TagHeartBtInt     = 108
	TagExecType       = 150
	TagLeavesQty      = 151
)

// MsgType constants
const (
	MsgTypeHeartbeat               = "0"
	MsgTypeTestRequest             = "1"
	MsgTypeResendRequest           = "2"
	MsgTypeReject                  = "3"
	MsgTypeSequenceReset           = "4"
	MsgTypeLogout                  = "5"
	MsgTypeExecutionReport         = "8"
	MsgTypeOrderCancelReject       = "9"
	MsgTypeLogon                   = "A"
	MsgTypeNewOrderSingle          = "D"
	MsgTypeOrderCancelRequest      = "F"
	MsgTypeOrderCancelReplaceRequest = "G"
)

// Message models a structured FIX Tag-Value protocol message.
type Message struct {
	tags   map[int]string
	fields []tagValue
}

type tagValue struct {
	tag   int
	value string
}

func NewMessage(msgType string) *Message {
	m := &Message{
		tags:   make(map[int]string),
		fields: make([]tagValue, 0),
	}
	m.Set(TagBeginString, "FIX.4.2")
	m.Set(TagMsgType, msgType)
	return m
}

func (m *Message) Set(tag int, value string) {
	m.tags[tag] = value
	for i, f := range m.fields {
		if f.tag == tag {
			m.fields[i].value = value
			return
		}
	}
	m.fields = append(m.fields, tagValue{tag: tag, value: value})
}

func (m *Message) Get(tag int) string {
	return m.tags[tag]
}

func (m *Message) GetInt(tag int) (int64, error) {
	v, ok := m.tags[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.ParseInt(v, 10, 64)
}

func (m *Message) GetFloat(tag int) (float64, error) {
	v, ok := m.tags[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.ParseFloat(v, 64)
}

// ParseMessage parses a raw SOH-delimited FIX byte string.
func ParseMessage(raw []byte) (*Message, error) {
	m := &Message{
		tags:   make(map[int]string),
		fields: make([]tagValue, 0),
	}

	parts := bytes.Split(raw, []byte{SOH})
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		eqIdx := bytes.IndexByte(part, '=')
		if eqIdx == -1 {
			continue
		}
		tagStr := string(part[:eqIdx])
		valStr := string(part[eqIdx+1:])

		tag, err := strconv.Atoi(tagStr)
		if err != nil {
			return nil, fmt.Errorf("invalid FIX tag %s: %w", tagStr, err)
		}
		m.Set(tag, valStr)
	}

	if m.Get(TagMsgType) == "" {
		return nil, fmt.Errorf("missing Tag 35 (MsgType)")
	}
	return m, nil
}

// Serialize formats the message into standard FIX wire format with BodyLength and Checksum.
func (m *Message) Serialize() []byte {
	// Build body content excluding Tag 8 (BeginString), Tag 9 (BodyLength), and Tag 10 (Checksum)
	var bodyBuf strings.Builder
	for _, f := range m.fields {
		if f.tag == TagBeginString || f.tag == TagBodyLength || f.tag == TagChecksum {
			continue
		}
		bodyBuf.WriteString(fmt.Sprintf("%d=%s%c", f.tag, f.value, SOH))
	}
	bodyStr := bodyBuf.String()

	beginString := m.Get(TagBeginString)
	if beginString == "" {
		beginString = "FIX.4.2"
	}

	header := fmt.Sprintf("%d=%s%c%d=%d%c", TagBeginString, beginString, SOH, TagBodyLength, len(bodyStr), SOH)
	rawMsgWithoutChecksum := header + bodyStr

	// Calculate checksum: sum of all bytes modulo 256
	var checkSum uint32
	for i := 0; i < len(rawMsgWithoutChecksum); i++ {
		checkSum += uint32(rawMsgWithoutChecksum[i])
	}
	checkSumMod := checkSum % 256

	finalStr := fmt.Sprintf("%s%d=%03d%c", rawMsgWithoutChecksum, TagChecksum, checkSumMod, SOH)
	return []byte(finalStr)
}

// FormatSendingTime produces standard UTC timestamp for Tag 52.
func FormatSendingTime(t time.Time) string {
	return t.UTC().Format("20060102-15:04:05.000")
}
