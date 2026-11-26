// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type testParent struct {
	ID string
}

type testChild struct {
	ID     string
	Parent string
}

type testGrandchild struct {
	ID      string
	ChildID string
}

// relationTestSchema builds a parent table and a child table referencing it.
// When restrict is true the parent blocks deletion while children exist.
func relationTestSchema(action ReferenceAction) *DBSchema {
	return &DBSchema{
		Tables: map[string]*TableSchema{
			"parent": {
				Name: "parent",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
				},
				Relations: []*RelationSchema{
					{
						Table:    "child",
						Index:    "parent",
						OnDelete: action,
					},
				},
			},
			"child": {
				Name: "child",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"parent": {
						Name:    "parent",
						Indexer: &StringFieldIndex{Field: "Parent"},
					},
				},
			},
		},
	}
}

func countRows(t *testing.T, txn *Txn, table string) int {
	t.Helper()
	iter, err := txn.Get(table, "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	count := 0
	for raw := iter.Next(); raw != nil; raw = iter.Next() {
		count++
	}
	return count
}

func TestRelation_CascadeDelete(t *testing.T) {
	db, err := NewMemDB(relationTestSchema(Cascade))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	if err := txn.Insert("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, id := range []string{"c1", "c2", "c3"} {
		if err := txn.Insert("child", &testChild{ID: id, Parent: "p1"}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	// An unrelated parent and child must be left alone.
	if err := txn.Insert("parent", &testParent{ID: "p2"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("child", &testChild{ID: "c4", Parent: "p2"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	txn = db.Txn(true)
	if err := txn.Delete("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()

	if raw, err := read.First("parent", "id", "p1"); err != nil || raw != nil {
		t.Fatalf("parent p1 should be deleted, got %#v err %v", raw, err)
	}
	for _, id := range []string{"c1", "c2", "c3"} {
		if raw, err := read.First("child", "id", id); err != nil || raw != nil {
			t.Fatalf("child %s should be cascade deleted, got %#v err %v", id, raw, err)
		}
	}
	if raw, err := read.First("parent", "id", "p2"); err != nil || raw == nil {
		t.Fatalf("unrelated parent p2 should remain")
	}
	if raw, err := read.First("child", "id", "c4"); err != nil || raw == nil {
		t.Fatalf("unrelated child c4 should remain")
	}
}

func TestRelation_RestrictBlocksDelete(t *testing.T) {
	db, err := NewMemDB(relationTestSchema(Restrict))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	if err := txn.Insert("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("child", &testChild{ID: "c1", Parent: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	txn = db.Txn(true)
	err = txn.Delete("parent", &testParent{ID: "p1"})
	if !errors.Is(err, ErrReferenceExists) {
		t.Fatalf("expected ErrReferenceExists, got %v", err)
	}
	// The failed transaction must be discarded; nothing was committed.
	txn.Abort()

	read := db.Txn(false)
	defer read.Abort()

	if raw, err := read.First("parent", "id", "p1"); err != nil || raw == nil {
		t.Fatalf("restricted parent must remain")
	}
	if raw, err := read.First("child", "id", "c1"); err != nil || raw == nil {
		t.Fatalf("referencing child must remain")
	}

	// Once the referencing child is gone, deletion succeeds.
	txn = db.Txn(true)
	if err := txn.Delete("child", &testChild{ID: "c1", Parent: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Delete("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("delete should succeed after children removed: %v", err)
	}
	txn.Commit()

	read2 := db.Txn(false)
	defer read2.Abort()
	if raw, _ := read2.First("parent", "id", "p1"); raw != nil {
		t.Fatalf("parent should be deleted once unrestricted")
	}
}

func TestRelation_UndeclaredTableKeepsLegacyBehavior(t *testing.T) {
	// Neither table declares a relation, so deleting a parent leaves the
	// orphaned child untouched exactly as before.
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"parent": {
				Name: "parent",
				Indexes: map[string]*IndexSchema{
					"id": {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
				},
			},
			"child": {
				Name: "child",
				Indexes: map[string]*IndexSchema{
					"id":     {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
					"parent": {Name: "parent", Indexer: &StringFieldIndex{Field: "Parent"}},
				},
			},
		},
	}

	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	if err := txn.Insert("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("child", &testChild{ID: "c1", Parent: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	txn = db.Txn(true)
	if err := txn.Delete("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()
	if raw, _ := read.First("parent", "id", "p1"); raw != nil {
		t.Fatalf("parent should be deleted")
	}
	if raw, _ := read.First("child", "id", "c1"); raw == nil {
		t.Fatalf("orphaned child must remain when no relation declared")
	}
}

func TestRelation_MultiLevelCascade(t *testing.T) {
	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"parent": {
				Name: "parent",
				Indexes: map[string]*IndexSchema{
					"id": {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
				},
				Relations: []*RelationSchema{
					{Table: "child", Index: "parent", OnDelete: Cascade},
				},
			},
			"child": {
				Name: "child",
				Indexes: map[string]*IndexSchema{
					"id":     {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
					"parent": {Name: "parent", Indexer: &StringFieldIndex{Field: "Parent"}},
				},
				Relations: []*RelationSchema{
					{Table: "grandchild", Index: "child", OnDelete: Cascade},
				},
			},
			"grandchild": {
				Name: "grandchild",
				Indexes: map[string]*IndexSchema{
					"id":    {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
					"child": {Name: "child", Indexer: &StringFieldIndex{Field: "ChildID"}},
				},
			},
		},
	}

	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	_ = txn.Insert("parent", &testParent{ID: "p1"})
	_ = txn.Insert("child", &testChild{ID: "c1", Parent: "p1"})
	_ = txn.Insert("grandchild", &testGrandchild{ID: "g1", ChildID: "c1"})
	_ = txn.Insert("grandchild", &testGrandchild{ID: "g2", ChildID: "c1"})
	txn.Commit()

	txn = db.Txn(true)
	if err := txn.Delete("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()
	if countRows(t, read, "parent") != 0 ||
		countRows(t, read, "child") != 0 ||
		countRows(t, read, "grandchild") != 0 {
		t.Fatalf("expected all rows cascaded away")
	}
}

func TestRelation_LargeCascadeStreams(t *testing.T) {
	db, err := NewMemDB(relationTestSchema(Cascade))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	const children = 50000
	txn := db.Txn(true)
	if err := txn.Insert("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 0; i < children; i++ {
		if err := txn.Insert("child", &testChild{ID: fmt.Sprintf("c%d", i), Parent: "p1"}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	txn.Commit()

	txn = db.Txn(true)
	if err := txn.Delete("parent", &testParent{ID: "p1"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()
	if got := countRows(t, read, "child"); got != 0 {
		t.Fatalf("expected %d children deleted, %d remain", children, got)
	}
	if got := countRows(t, read, "parent"); got != 0 {
		t.Fatalf("parent remains: %d", got)
	}
}

func TestRelation_InvalidSchema(t *testing.T) {
	cases := map[string]*DBSchema{
		"unknown table": {
			Tables: map[string]*TableSchema{
				"parent": {
					Name: "parent",
					Indexes: map[string]*IndexSchema{
						"id": {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
					},
					Relations: []*RelationSchema{
						{Table: "missing", Index: "id", OnDelete: Cascade},
					},
				},
			},
		},
		"unknown index": func() *DBSchema {
			schema := relationTestSchema(Cascade)
			schema.Tables["parent"].Relations[0].Index = "nope"
			return schema
		}(),
		"unknown action": func() *DBSchema {
			schema := relationTestSchema(Cascade)
			schema.Tables["parent"].Relations[0].OnDelete = ReferenceAction(99)
			return schema
		}(),
	}

	for name, schema := range cases {
		if err := schema.Validate(); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
	}
}

// TestRelation_SelfReferenceCycle makes sure a table that references itself
// terminates instead of looping forever when a cascade reaches a row it has
// already scheduled for deletion.
func TestRelation_SelfReferenceCycle(t *testing.T) {
	type node struct {
		ID     string
		Parent string
	}

	schema := &DBSchema{
		Tables: map[string]*TableSchema{
			"node": {
				Name: "node",
				Indexes: map[string]*IndexSchema{
					"id":     {Name: "id", Unique: true, Indexer: &StringFieldIndex{Field: "ID"}},
					"parent": {Name: "parent", AllowMissing: true, Indexer: &StringFieldIndex{Field: "Parent"}},
				},
				Relations: []*RelationSchema{
					{Table: "node", Index: "parent", OnDelete: Cascade},
				},
			},
		},
	}

	db, err := NewMemDB(schema)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	if err := txn.Insert("node", &node{ID: "root"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("node", &node{ID: "a", Parent: "root"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := txn.Insert("node", &node{ID: "b", Parent: "a"}); err != nil {
		t.Fatalf("err: %v", err)
	}
	txn.Commit()

	done := make(chan struct{})
	go func() {
		del := db.Txn(true)
		err = del.Delete("node", &node{ID: "root"})
		if err == nil {
			del.Commit()
		} else {
			del.Abort()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("cascade did not terminate on a self-referencing cycle")
	}
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	read := db.Txn(false)
	defer read.Abort()
	if got := countRows(t, read, "node"); got != 0 {
		t.Fatalf("expected all nodes cascaded away, %d remain", got)
	}
}

// TestRelation_DeleteAllCascades confirms DeleteAll inherits relation
// handling because it deletes through Txn.Delete.
func TestRelation_DeleteAllCascades(t *testing.T) {
	db, err := NewMemDB(relationTestSchema(Cascade))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	txn := db.Txn(true)
	_ = txn.Insert("parent", &testParent{ID: "p1"})
	_ = txn.Insert("parent", &testParent{ID: "p2"})
	_ = txn.Insert("child", &testChild{ID: "c1", Parent: "p1"})
	_ = txn.Insert("child", &testChild{ID: "c2", Parent: "p2"})
	txn.Commit()

	txn = db.Txn(true)
	deleted, err := txn.DeleteAll("parent", "id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 parents deleted, got %d", deleted)
	}
	txn.Commit()

	read := db.Txn(false)
	defer read.Abort()
	if got := countRows(t, read, "child"); got != 0 {
		t.Fatalf("expected children cascaded, %d remain", got)
	}
}
