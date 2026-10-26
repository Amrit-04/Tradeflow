package fix

import (
	"fmt"
	"sync"
	"time"

	"tradeflow/internal/domain"
	"tradeflow/internal/events"
)

// Session represents a stateful FIX client connection.
type Session struct {
	mu           sync.Mutex
	SenderCompID string
	TargetCompID string
	InSeqNum     int64
	OutSeqNum    int64
	IsLoggedOn   bool
	LastActivity time.Time
}

func NewSession(senderCompID string, targetCompID string) *Session {
	return &Session{
		SenderCompID: senderCompID,
		TargetCompID: targetCompID,
		InSeqNum:     0,
		OutSeqNum:    1,
		IsLoggedOn:   false,
		LastActivity: time.Now().UTC(),
	}
}

// NextOutMsg builds a message envelope with standard session header tags.
func (s *Session) NextOutMsg(msgType string) *Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg := NewMessage(msgType)
	msg.Set(TagSenderCompID, s.SenderCompID)
	msg.Set(TagTargetCompID, s.TargetCompID)
	msg.Set(TagMsgSeqNum, fmt.Sprintf("%d", s.OutSeqNum))
	msg.Set(TagSendingTime, FormatSendingTime(time.Now().UTC()))
	s.OutSeqNum++
	return msg
}

// ProcessInbound handles an incoming raw message and returns any immediate reply message.
func (s *Session) ProcessInbound(raw []byte, onOrder func(events.OrderInboundPayload) error) (*Message, error) {
	msg, err := ParseMessage(raw)
	if err != nil {
		return nil, fmt.Errorf("malformed FIX message: %w", err)
	}

	s.mu.Lock()
	s.InSeqNum++
	s.LastActivity = time.Now().UTC()
	s.mu.Unlock()

	msgType := msg.Get(TagMsgType)
	switch msgType {
	case MsgTypeLogon:
		s.mu.Lock()
		s.IsLoggedOn = true
		s.mu.Unlock()
		reply := s.NextOutMsg(MsgTypeLogon)
		reply.Set(TagHeartBtInt, "30")
		return reply, nil

	case MsgTypeHeartbeat:
		return nil, nil // Heartbeat acked

	case MsgTypeTestRequest:
		testReqID := msg.Get(112)
		reply := s.NextOutMsg(MsgTypeHeartbeat)
		if testReqID != "" {
			reply.Set(112, testReqID)
		}
		return reply, nil

	case MsgTypeNewOrderSingle:
		clOrdID := msg.Get(TagClOrdID)
		symbol := msg.Get(TagSymbol)
		sideStr := msg.Get(TagSide)
		qtyVal, _ := msg.GetInt(TagOrderQty)
		pxFloat, _ := msg.GetFloat(TagPrice)
		account := msg.Get(TagAccount)
		if account == "" {
			account = "DEFAULT_ACCT"
		}

		var side domain.Side
		if sideStr == "1" {
			side = domain.SideBuy
		} else {
			side = domain.SideSell
		}

		orderPayload := events.OrderInboundPayload{
			OrderID:   fmt.Sprintf("ORD-%s", clOrdID),
			ClOrdID:   clOrdID,
			Account:   account,
			Symbol:    symbol,
			Side:      side,
			OrderType: domain.OrderTypeLimit,
			OrderQty:  domain.Qty(qtyVal),
			Price:     domain.DollarsToPrice(pxFloat),
			Timestamp: time.Now().UTC(),
		}

		if onOrder != nil {
			if err := onOrder(orderPayload); err != nil {
				// Return reject
				rej := s.NextOutMsg(MsgTypeExecutionReport)
				rej.Set(TagOrderID, orderPayload.OrderID)
				rej.Set(TagClOrdID, clOrdID)
				rej.Set(TagExecID, fmt.Sprintf("EX-REJ-%d", time.Now().UnixNano()))
				rej.Set(TagExecType, "8") // Rejected
				rej.Set(TagOrdStatus, "8") // Rejected
				rej.Set(TagText, err.Error())
				return rej, nil
			}
		}

		// Acknowledge receipt: ExecutionReport (ExecType=0 New, OrdStatus=0 New)
		ack := s.NextOutMsg(MsgTypeExecutionReport)
		ack.Set(TagOrderID, orderPayload.OrderID)
		ack.Set(TagClOrdID, clOrdID)
		ack.Set(TagExecID, fmt.Sprintf("EX-%d", time.Now().UnixNano()))
		ack.Set(TagExecType, "0") // New
		ack.Set(TagOrdStatus, "0") // New
		ack.Set(TagSymbol, symbol)
		ack.Set(TagSide, sideStr)
		ack.Set(TagOrderQty, fmt.Sprintf("%d", qtyVal))
		ack.Set(TagPrice, fmt.Sprintf("%.2f", pxFloat))
		ack.Set(TagCumQty, "0")
		ack.Set(TagLeavesQty, fmt.Sprintf("%d", qtyVal))
		ack.Set(TagAvgPx, "0.00")
		return ack, nil

	default:
		return nil, nil
	}
}
