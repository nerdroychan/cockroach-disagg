// Code generated by "stringer"; DO NOT EDIT.

package sql

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[noEvent-0]
	_ = x[txnStart-1]
	_ = x[txnCommit-2]
	_ = x[txnRollback-3]
	_ = x[txnRestart-4]
	_ = x[txnUpgradeToExplicit-5]
}

const _txnEventType_name = "noEventtxnStarttxnCommittxnRollbacktxnRestarttxnUpgradeToExplicit"

var _txnEventType_index = [...]uint8{0, 7, 15, 24, 35, 45, 65}

func (i txnEventType) String() string {
	if i < 0 || i >= txnEventType(len(_txnEventType_index)-1) {
		return "txnEventType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _txnEventType_name[_txnEventType_index[i]:_txnEventType_index[i+1]]
}
