import { MongoClient } from 'mongodb';

let connection;

export const connect = async (options, onSuccess, onFailure) => {
  console.log("connection url : ",options.url);
  const client = new MongoClient(options.url);

  try {
    // Connect the client to the server (optional starting in v4.7)
    connection = await client.connect();

    console.info("DB connection successful.");
    return onSuccess();
  } catch(ex) {
    console.error("DB connection failed.");
    return onFailure(ex);
  }
}

//Fetch documents
export const findAll = async (db, coll, query = {}) => {
  const dbc = await connection.db(db);
  return new Promise((resolve, reject) => {
    dbc.collection(coll).find(query).toArray((e, d) => {
      if(e) reject(e);
      resolve(d);
    });
  });
}

export const findOne = async (db, coll, query = {}) => {
  const dbc = await connection.db(db);
  return new Promise((resolve, reject) => {
    db.collection(coll).findOne(query).toArray((e, d) => {
      if(e) reject(e);
      resolve(d);
    });
  });
}

//Update Documents
export const updateOne = async (db, coll, query = {}, update) => {
  const dbc = await connection.db(db);
  return dbc.collection(coll).updateOne(query, update);
}

export const updateMany = async (db, coll, query = {}, update) => {
  const dbc = await connection.db(db);
  return dbc.collection(coll).updateMany(query, update);
}

//Insert Documents
export const insertOne = async (db, coll, doc) => {
  const dbc = await connection.db(db);
  return dbc.collection(coll).insertOne(doc);
}

export const insertMany = async (db, coll, docArr) => {
  const dbc = await connection.db(db);
  return dbc.collection(coll).insertMany(docArr);
}

//Delete Documents
export const deleteOne = async (db, coll, query) => {
  const dbc = await connection.db(db);
  return dbc.collection(coll).deleteOne(query);
}

export const deleteMany = async (db, coll, query) => {
  const dbc = await connection.db(db);
  return dbc.collection(coll).deleteMany(query);
}