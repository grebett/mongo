// This package provides a wrapper over mgo package
package mongo

import (
	"errors"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// This struct holds the session and the collections handlers
type Mongo struct {
	Session     *mgo.Session
	Collections map[string]*mgo.Collection
}

// This method returns a new mongoDB session
func (m *Mongo) NewSession(url string) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	} else {
		// stocking created session
		m.Session = session
		// initialize collections map
		m.Collections = make(map[string]*mgo.Collection)
		return nil
	}
}

// This method returns a new access to an existing collection. If collection does not exist, mongoDB creates it. The collection is stocked in the Collections map
func (m *Mongo) NewCollection(databaseName string, collectionName string) *mgo.Collection {
	database := m.Session.DB(databaseName)
	collectionAccess := database.C(collectionName)
	m.Collections[databaseName+"."+collectionName] = collectionAccess
	return collectionAccess
}

func (m *Mongo) ObjectId(_id string) (bson.ObjectId, error) {
	if len(_id) != 24 {
		return "", errors.New("ObjectId must be 24 hexademical characters long (to be converted to 12 bytes)")
	}
	return bson.ObjectIdHex(_id), nil
}

// CRUD METHOD
// This method finds the matching document based on the provided ObjectId - if filter is nil, returns the all document
func (m *Mongo) FindById(collectionName string, _id bson.ObjectId, selected bson.M) (bson.M, error) {
	var err error

	// formatting query (bson.M is a shortcut to a map creation)
	query := bson.M{"_id": _id}

	// mongodb request
	var result bson.M

	if selected != nil {
		// care, if selected is map[], will return the whole document
		err = m.Collections[collectionName].Find(query).Select(selected).One(&result)
	} else {
		err = m.Collections[collectionName].Find(query).One(&result)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CRUD METHOD
// This method finds one document contained in the collection matching the query - if filter is nil, returns the whole documents
func (m *Mongo) FindOne(collectionName string, query bson.M, selected bson.M) (bson.M, error) {
	// mongodb request
	var (
		result bson.M
		err    error
	)

	if selected != nil {
		// care, if filter is map[], will return the whole document
		err = m.Collections[collectionName].Find(query).Select(query).One(&result)
	} else {
		err = m.Collections[collectionName].Find(query).One(&result)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CRUD METHOD
// This method finds all the document contained in the collection
func (m *Mongo) FindAll(collectionName string, query bson.M, selected bson.M) ([]bson.M, error) {
	// mongodb request
	var (
		result []bson.M
		err    error
	)

	if selected != nil {
		// care, if selected is map[], will return the whole document
		err = m.Collections[collectionName].Find(query).Select(selected).All(&result)
	} else {
		err = m.Collections[collectionName].Find(query).All(&result)
	}

	if err != nil {
		return nil, err
	}
	return result, nil
}

// CRUD METHOD
// This method insert a new document into the collection
func (m *Mongo) Insert(collectionName string, document interface{}) (bool, error) {
	if err := m.Collections[collectionName].Insert(document); err != nil {
		return false, err
	}
	return true, nil
}

// CRUD METHOD
// This method insert a new document into the collection
// update data must be a map[string](map[string]interface{})
// first property is the $operator (such as $set or $inc)
func (m *Mongo) Update(collectionName string, _id bson.ObjectId, update interface{}) (bool, error) {
	if err := m.Collections[collectionName].Update(bson.M{"_id": _id}, update); err != nil { // could be UpdateId
		return false, err
	}
	return true, nil
}

// CRUD METHOD
// This method deletes the matching document based on the provided ObjectId
func (m *Mongo) Delete(collectionName string, _id bson.ObjectId) (bool, error) {
	if err := m.Collections[collectionName].RemoveId(_id); err != nil {
		return false, err
	}
	return true, nil
}

// This method deletes the matching document based on the provided ObjectId
func (m *Mongo) DeleteAll(collectionName string, filter bson.M) (bool, error) {
	if _, err := m.Collections[collectionName].RemoveAll(filter); err != nil {
		return false, err
	}
	return true, nil
}
