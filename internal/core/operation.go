package core

// OperationKind is used to identify what kind of operation is being performed by migration.
type OperationKind string

const (
	OperationSQL        OperationKind = "SQL"
	OperationNote       OperationKind = "NOTE"
	OperationBreaking   OperationKind = "BREAKING"
	OperationUnresolved OperationKind = "UNRESOLVED"
)

// OperationRisk is used to identify the risk level of an operation.
type OperationRisk string

const (
	RiskInfo     OperationRisk = "INFO"
	RiskWarning  OperationRisk = "WARNING"
	RiskBreaking OperationRisk = "BREAKING"
	RiskCritical OperationRisk = "CRITICAL"
)

// Operation struct contains all information about a single operation of migration.
// It also contains a rollback SQL statement and a risk level.
type Operation struct {
	Kind OperationKind `json:"kind"`

	SQL         string `json:"sql,omitempty"`
	RollbackSQL string `json:"rollbackSql,omitempty"`

	Risk         OperationRisk `json:"risk,omitempty"`
	RequiresLock bool          `json:"requiresLock,omitempty"`

	UnresolvedReason string `json:"unresolvedReason,omitempty"`
}
