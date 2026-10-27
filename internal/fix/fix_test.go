package fix_test

import (
	"errors"
	"testing"

	"tradeflow/internal/domain"
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

func TestFIXCancelOrderRequest(t *testing.T) {
	session := fix.NewSession("TRADEFLOW", "CLIENT_A")
	session.IsLoggedOn = true

	// Success case
	var capturedOrderID, capturedOrigClOrdID, capturedClOrdID string
	session.OnCancel = func(orderID, origClOrdID, clOrdID string) error {
		capturedOrderID = orderID
		capturedOrigClOrdID = origClOrdID
		capturedClOrdID = clOrdID
		return nil
	}

	cxlMsg := fix.NewMessage(fix.MsgTypeOrderCancelRequest)
	cxlMsg.Set(fix.TagClOrdID, "CXL-001")
	cxlMsg.Set(fix.TagOrigClOrdID, "CL-ORIG-001")
	cxlMsg.Set(fix.TagOrderID, "ORD-ORIG-001")
	cxlMsg.Set(fix.TagSymbol, "AAPL")
	cxlMsg.Set(fix.TagSide, "1")

	reply, err := session.ProcessInbound(cxlMsg.Serialize(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply.Get(fix.TagMsgType) != fix.MsgTypeExecutionReport {
		t.Fatalf("expected ExecutionReport, got %s", reply.Get(fix.TagMsgType))
	}
	if reply.Get(fix.TagExecType) != "4" || reply.Get(fix.TagOrdStatus) != "4" {
		t.Fatalf("expected ExecType=4 OrdStatus=4 (Cancelled), got %s / %s", reply.Get(fix.TagExecType), reply.Get(fix.TagOrdStatus))
	}
	if capturedOrderID != "ORD-ORIG-001" || capturedOrigClOrdID != "CL-ORIG-001" || capturedClOrdID != "CXL-001" {
		t.Fatalf("captured fields mismatch: id=%s orig=%s cxl=%s", capturedOrderID, capturedOrigClOrdID, capturedClOrdID)
	}

	// Reject case (e.g. order already filled or terminal)
	session.OnCancel = func(orderID, origClOrdID, clOrdID string) error {
		return errors.New("order is already in terminal state Filled")
	}

	rejReply, err := session.ProcessInbound(cxlMsg.Serialize(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rejReply.Get(fix.TagMsgType) != fix.MsgTypeOrderCancelReject {
		t.Fatalf("expected OrderCancelReject (9), got %s", rejReply.Get(fix.TagMsgType))
	}
	if rejReply.Get(fix.TagCxlRejResponseTo) != "1" {
		t.Fatalf("expected CxlRejResponseTo=1, got %s", rejReply.Get(fix.TagCxlRejResponseTo))
	}
}

func TestFIXCancelReplaceOrderRequest(t *testing.T) {
	session := fix.NewSession("TRADEFLOW", "CLIENT_A")
	session.IsLoggedOn = true

	// Success case
	session.OnReplace = func(orderID, origClOrdID, clOrdID string, newQty domain.Qty, newPrice domain.Price) error {
		if newQty != 250 || newPrice != 15500 {
			return errors.New("qty or price mismatch")
		}
		return nil
	}

	repMsg := fix.NewMessage(fix.MsgTypeOrderCancelReplaceRequest)
	repMsg.Set(fix.TagClOrdID, "REP-001")
	repMsg.Set(fix.TagOrigClOrdID, "CL-ORIG-001")
	repMsg.Set(fix.TagOrderID, "ORD-ORIG-001")
	repMsg.Set(fix.TagSymbol, "AAPL")
	repMsg.Set(fix.TagSide, "1")
	repMsg.Set(fix.TagOrderQty, "250")
	repMsg.Set(fix.TagPrice, "155.00")

	reply, err := session.ProcessInbound(repMsg.Serialize(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply.Get(fix.TagMsgType) != fix.MsgTypeExecutionReport {
		t.Fatalf("expected ExecutionReport, got %s", reply.Get(fix.TagMsgType))
	}
	if reply.Get(fix.TagExecType) != "5" || reply.Get(fix.TagOrdStatus) != "5" {
		t.Fatalf("expected ExecType=5 OrdStatus=5 (Replaced), got %s / %s", reply.Get(fix.TagExecType), reply.Get(fix.TagOrdStatus))
	}
	if reply.Get(fix.TagOrderQty) != "250" || reply.Get(fix.TagPrice) != "155.00" {
		t.Fatalf("expected updated qty and price on ack, got qty=%s px=%s", reply.Get(fix.TagOrderQty), reply.Get(fix.TagPrice))
	}

	// Reject case
	session.OnReplace = func(orderID, origClOrdID, clOrdID string, newQty domain.Qty, newPrice domain.Price) error {
		return errors.New("cannot replace: price deviates by 15%")
	}

	rejReply, err := session.ProcessInbound(repMsg.Serialize(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rejReply.Get(fix.TagMsgType) != fix.MsgTypeOrderCancelReject {
		t.Fatalf("expected OrderCancelReject (9), got %s", rejReply.Get(fix.TagMsgType))
	}
	if rejReply.Get(fix.TagCxlRejResponseTo) != "2" {
		t.Fatalf("expected CxlRejResponseTo=2, got %s", rejReply.Get(fix.TagCxlRejResponseTo))
	}
}

func TestFIXResendRequestUnsupported(t *testing.T) {
	session := fix.NewSession("TRADEFLOW", "CLIENT_A")
	session.IsLoggedOn = true

	resendMsg := fix.NewMessage(fix.MsgTypeResendRequest)
	resendMsg.Set(fix.TagMsgSeqNum, "10")

	reply, err := session.ProcessInbound(resendMsg.Serialize(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply.Get(fix.TagMsgType) != fix.MsgTypeReject {
		t.Fatalf("expected Reject (3), got %s", reply.Get(fix.TagMsgType))
	}
	if reply.Get(fix.TagText) == "" {
		t.Fatalf("expected explanation in TagText explaining resend is unsupported")
	}
}
