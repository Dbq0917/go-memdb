// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"errors"
	"fmt"
)

// ReferenceAction determines what happens to referencing rows when the row
// they point to is deleted.
type ReferenceAction int

const (
	// Cascade deletes every object in the referencing table that points to
	// the deleted object, following any further relations declared on those
	// tables.
	Cascade ReferenceAction = iota

	// Restrict prevents the deletion while at least one object in the
	// referencing table still points to the object being deleted.
	Restrict
)

// ErrReferenceExists is returned by Delete (and helpers built on it such as
// DeleteAll) when a relation uses the Restrict action and at least one
// referencing object still exists. Use errors.Is to detect it.
var ErrReferenceExists = errors.New("cannot delete object that is still referenced")

// RelationSchema declares how objects in another table reference the objects
// stored in the table that owns the relation. Relations are optional: tables
// without any relations keep the historical behavior where deleting an object
// leaves any objects that happen to reference it untouched.
type RelationSchema struct {
	// Table is the name of the referencing ("child") table.
	Table string

	// Index is the index on the referencing table whose values match the
	// primary id of an object in this table.
	Index string

	// OnDelete selects how referencing objects are handled when the object
	// they point to is deleted. It defaults to Cascade.
	OnDelete ReferenceAction
}

// validate verifies a single relation against the full database schema so
// that cross-table references can be checked.
func (r *RelationSchema) validate(parent string, schema *DBSchema) error {
	if r == nil {
		return fmt.Errorf("relation on table %q is nil", parent)
	}

	if r.Table == "" {
		return fmt.Errorf("relation on table %q is missing the referencing table name", parent)
	}

	child, ok := schema.Tables[r.Table]
	if !ok {
		return fmt.Errorf("relation on table %q references unknown table %q", parent, r.Table)
	}

	index, ok := child.Indexes[r.Index]
	if !ok {
		return fmt.Errorf(
			"relation on table %q references unknown index %q on table %q",
			parent, r.Index, r.Table,
		)
	}

	if _, ok := index.Indexer.(SingleIndexer); !ok {
		return fmt.Errorf(
			"relation on table %q references index %q on table %q which must use a SingleIndexer",
			parent, r.Index, r.Table,
		)
	}

	switch r.OnDelete {
	case Cascade, Restrict:
	default:
		return fmt.Errorf(
			"relation on table %q -> table %q has unknown OnDelete action %d",
			parent, r.Table, r.OnDelete,
		)
	}

	return nil
}

// validateRelations checks every relation declared across all tables.
func validateRelations(schema *DBSchema) error {
	for name, table := range schema.Tables {
		for _, relation := range table.Relations {
			if err := relation.validate(name, schema); err != nil {
				return err
			}
		}
	}
	return nil
}
