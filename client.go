// Package db provides helper functions that make interfacing with the MongoDB Go driver library easier
package mongoboiler

import (
	"context"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type DB struct {
	db     *mongo.Database
	client *mongo.Client
	ctx    context.Context
}

func New(client *mongo.Client, name string, ctx context.Context) *DB {
	return &DB{client.Database(name), client, ctx}
}

// Collection is the wrapper for Mongo Collection
type Collection struct {
	*DB
	collection *mongo.Collection
}

func (wrapper *DB) NewCollection(collectionName string) *Collection {
	return &Collection{wrapper, wrapper.db.Collection(collectionName)}
}

// Drop drops the current Collection (collection)
func (c Collection) Drop() error {
	return c.collection.Drop(c.ctx)
}

// FindOne finds first document that satisfies filter and fills res with the un marshaled document.
func (c Collection) FindOne(filter bson.D, res any) error {
	err := c.collection.FindOne(c.ctx, filter).Decode(res)
	if err != nil {
		return err
	}
	return nil
}

// FindMany iterates cursor of all docs matching filter and fills res with un marshalled documents.
func (c Collection) FindMany(filter bson.D, res *[]any) error {
	arrType := reflect.TypeOf(res).Elem()
	cursor, err := c.collection.Find(c.ctx, filter)

	ctx := c.ctx
	for cursor.Next(ctx) {
		doc := reflect.New(arrType).Interface()
		err := cursor.Decode(&doc)
		if err != nil {
			return err
		}
		*res = append(*res, doc)
	}

	// un marshall fail
	if cursor.Err() != nil {
		return err
	}

	// Close cursor after we're done with it
	cursor.Close(ctx)
	return nil
}

// UpdateOne updates single document matching filter and applies update to it.
// Returns number of documents matched and modified. Should always be either 0 or 1.
func (c Collection) UpdateOne(filter, update bson.D) (int64, int64, error) {
	updateRes, err := c.collection.UpdateOne(c.ctx, filter, update)
	if err != nil {
		return 0, 0, err
	}
	return updateRes.MatchedCount, updateRes.ModifiedCount, nil
}

// UpdateMany updates all documents matching the filter by applying the update query on it.
// Returns number of documents matched and modified.
func (c Collection) UpdateMany(filter, update bson.D) (int64, int64, error) {
	updateRes, err := c.collection.UpdateMany(c.ctx, filter, update)
	if err != nil {
		return 0, 0, err
	}
	return updateRes.MatchedCount, updateRes.ModifiedCount, nil
}

// InsertOne inserts a single struct as a document into the database and returns its ID.
// Returns inserted ID
func (c Collection) InsertOne(new any) (any, error) {
	insertRes, err := c.collection.InsertOne(c.ctx, new)
	if err != nil {
		return "", err
	}
	return insertRes.InsertedID, nil
}

// InsertMany takes a slice of structs, inserts them into the database.
// Returns list of inserted IDs
func (c Collection) InsertMany(new []any) (any, error) {
	insertRes, err := c.collection.InsertMany(c.ctx, new)
	if err != nil {
		return "", err
	}
	return insertRes.InsertedIDs, nil
}

// DeleteOne deletes single document that match the bson.D filter
func (c Collection) DeleteOne(filter bson.D) error {
	_, err := c.collection.DeleteOne(c.ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

// DeleteMany deletes all documents that match the bson.D filter
func (c Collection) DeleteMany(filter bson.D) error {
	_, err := c.collection.DeleteMany(c.ctx, filter)
	if err != nil {
		return err
	}
	return nil
}
