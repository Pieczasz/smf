package core

type OperationKind string

const (
	OperationSQL        OperationKind = "SQL"
	OperationNote       OperationKind = "NOTE"
	OperationBreaking   OperationKind = "BREAKING"
	OperationUnresolved OperationKind = "UNRESOLVED"
)

type OperationRisk string

const (
	RiskInfo     OperationRisk = "INFO"
	RiskWarning  OperationRisk = "WARNING"
	RiskBreaking OperationRisk = "BREAKING"
	RiskCritical OperationRisk = "CRITICAL"
)

type Operation struct {
	Kind OperationKind `json:"kind"`

	SQL         string `json:"sql,omitempty"`
	RollbackSQL string `json:"rollbackSql,omitempty"`

	Risk         OperationRisk `json:"risk,omitempty"`
	RequiresLock bool          `json:"requiresLock,omitempty"`

	UnresolvedReason string `json:"unresolvedReason,omitempty"`
}
