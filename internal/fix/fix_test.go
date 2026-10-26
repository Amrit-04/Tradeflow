package fix_test

import (
	"errors"
	"testing"

	"tradeflow/internal/events"
	"tradeflow/internal/fix"
)

func TestFIXMessageSerializeAndParse(t *testing.T) {
	msg := fix.NewMessage(fix.MsgTypeNewOrderSingle)
	msg.Set(fix.TagSenderCompID, "CLIENT_A")
	msg.Set(fix.TagTargetCompID, "TRADEFLOW")
	msg.Set(fix.TagMsgSeqNum, "1")
	msg.Set(fix.TagClOrdID, "ORDER_999")
	msg.Set(fix.TagSymbol, "AAPL")
	msg.Set(fix.TagSide, "1") // Buy
	msg.Set(fix.TagOrderQty, "500")
	msg.Set(fix.TagPrice, "150.50")

	raw := msg.Serialize()

	parsed, err := fix.ParseMessage(raw)
	if err != nil {
		t.Fatalf("failed to parse serialized FIX message: %v", err)
	}

	if parsed.Get(fix.TagMsgType) != fix.MsgTypeNewOrderSingle {
		t.Fatalf("expected MsgType=D, got %s", parsed.Get(fix.TagMsgType))
	}
	if parsed.Get(fix.TagClOrdID) != "ORDER_999" {
		t.Fatalf("expected ClOrdID=ORDER_999, got %s", parsed.Get(fix.TagClOrdID))
	}
	if parsed.Get(fix.TagSymbol) != "AAPL" {
		t.Fatalf("expected Symbol=AAPL, got %s", parsed.Get(fix.TagSymbol))
	}
}

func TestFIXSessionLogonAndOrder(t *testing.T) {
	session := fix.NewSession("TRADEFLOW", "CLIENT_A")

	// 1. Inbound Logon
	logonMsg := fix.NewMessage(fix.MsgTypeLogon)
	logonMsg.Set(fix.TagSenderCompID, "CLIENT_A")
	logonMsg.Set(fix.TagTargetCompID, "TRADEFLOW")
	logonMsg.Set(fix.TagMsgSeqNum, "1")

	reply, err := session.ProcessInbound(logonMsg.Serialize(), nil)
	if err != nil {
		t.Fatalf("failed to process logon: %v", err)
	}
	if reply.Get(fix.TagMsgType) != fix.MsgTypeLogon {
		t.Fatalf("expected Logon reply, got %s", reply.Get(fix.TagMsgType))
	}
	if !session.IsLoggedOn {
		t.Fatalf("session should be logged on")
	}

	// 2. Inbound NewOrderSingle
	orderMsg := fix.NewMessage(fix.MsgTypeNewOrderSingle)
	orderMsg.Set(fix.TagClOrdID, "CL-1234")
	orderMsg.Set(fix.TagSymbol, "MSFT")
	orderMsg.Set(fix.TagSide, "1")
	orderMsg.Set(fix.TagOrderQty, "100")
	orderMsg.Set(fix.TagPrice, "420.00")

	var capturedOrder events.OrderInboundPayload
	reply, err = session.ProcessInbound(orderMsg.Serialize(), func(p events.OrderInboundPayload) error {
		capturedOrder = p
		return nil
	})
	if err != nil {
		t.Fatalf("failed to process NewOrderSingle: %v", err)
	}

	if reply.Get(fix.TagMsgType) != fix.MsgTypeExecutionReport {
		t.Fatalf("expected ExecutionReport, got %s", reply.Get(fix.TagMsgType))
	}
	if reply.Get(fix.TagExecType) != "0" || reply.Get(fix.TagOrdStatus) != "0" {
		t.Fatalf("expected ExecType=0, OrdStatus=0, got %s / %s", reply.Get(fix.TagExecType), reply.Get(fix.TagOrdStatus))
	}
	if capturedOrder.ClOrdID != "CL-1234" || capturedOrder.OrderQty != 100 {
		t.Fatalf("captured order mismatch: %+v", capturedOrder)
	}

	// 3. Inbound Rejected Order
	reply, err = session.ProcessInbound(orderMsg.Serialize(), func(p events.OrderInboundPayload) error {
		return errors.New("RC01: Order quantity exceeds limit")
	})
	if err != nil {
		t.Fatalf("unexpected process error: %v", err)
	}
	if reply.Get(fix.TagExecType) != "8" || reply.Get(fix.TagOrdStatus) != "8" {
		t.Fatalf("expected rejected ExecutionReport (ExecType=8), got %s", reply.Get(fix.TagExecType))
	}
}
