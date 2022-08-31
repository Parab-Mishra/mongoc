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

export const findAll = async (db, coll, query = {}) => {
  const dbc = await connection.db(db);
  return new Promise((resolve, reject) => {
    dbc.collection(coll).find(query).toArray((e, d) => {
      if(e) reject(e);
      resolve(d);
    });
  });
}