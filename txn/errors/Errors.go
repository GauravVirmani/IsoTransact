package errors

import "errors"

var ConflictErr = errors.New("transaction conflicts with other concurrent transaction, retry")
var EmptyTxnError = errors.New("empty write batch, nothing to commit")
