package types

// SourceColumn is one column of a source table as currently defined at the source.
type SourceColumn struct {
	// Name is the source column name, before ResolveColumnName.
	Name string
	Type DataType
	// ExistingRowValue is what rows stored before the column was added read for it; nil is NULL.
	ExistingRowValue any
	// ExistingRowValueUnknown is set when the source cannot tell what those rows read
	// (volatile default, table rewritten since, column no longer in the catalog).
	ExistingRowValueUnknown bool
}

// SourceColumnsChange carries a source table's full current column list.
type SourceColumnsChange struct {
	Columns []SourceColumn
	// KeepColumns are source columns the change stream never carries values for (e.g. generated
	// columns); they are never added from this change and never treated as removed.
	KeepColumns []string
	// SyncStart marks a change read from the source catalog when the sync started rather than from
	// the change stream; it is not ordered with the pending changes.
	SyncStart bool
}
