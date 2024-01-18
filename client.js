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

//ToDo: All validations in all functions for query, values and options
export const FindAllDocuments = async (db, coll) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .find({})
      .toArray()
      .then((d) => {
        logger.debug("FindAllDocuments: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("FindAllDocuments: ", e);
        reject(e);
      });
  });
};

export const FindDocsWithQuery = async (db, coll, query = {}, projection = {}, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .find(query, projection, options)
      .toArray()
      .then((d) => {
        logger.debug("FindDocsWithQuery: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("FindDocsWithQuery: ", e);
        reject(e);
      });
  });
};

export const FindOneDocument = async (db, coll, query = {}, projection = {}, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .findOne(query, projection, options)
      .then((d) => {
        logger.debug("FindOneDocument: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("FindOneDocument: ", e);
        reject(e);
      });
  });
};

export const InsertOneDocument = async (db, coll, doc, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .insertOne(doc, options)
      .then((d) => {
        logger.debug("InsertOneDocument: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("InsertOneDocument: ", e);
        reject(e);
      });
  });
};

export const InsertDocumentArray = async (db, coll, docArr, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .insertMany(docArr, options)
      .then((d) => {
        logger.debug("InsertDocumentArray: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("InsertDocumentArray: ", e);
        reject(e);
      });
  });
};

export const UpdateOneDocument = async (db, coll, query, values, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .updateOne(query, { $set: values }, options)
      .then((d) => {
        logger.debug("UpdateOneDocument: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("UpdateOneDocument: ", e);
        reject(e);
      });
  });
};

export const UpdateManyDocuments = async (db, coll, query, values, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .updateMany(query, { $set: values }, options)
      .then((d) => {
        logger.debug("UpdateManyDocuments: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("UpdateManyDocuments: ", e);
        reject(e);
      });
  });
};

export const ModifyOneDocument = async (db, coll, query, values, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .updateOne(query, values, options)
      .then((d) => {
        logger.debug("ModifyOneDocument: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("ModifyOneDocument: ", e);
        reject(e);
      });
  });
};

export const ModifyManyDocuments = async (db, coll, query, values, options = {}) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .updateMany(query, values, options)
      .then((d) => {
        logger.debug("ModifyManyDocuments: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("ModifyManyDocuments: ", e);
        reject(e);
      });
  });
};

export const DeleteOneDocument = async (db, coll, query, values = {}) => {
  values.deleted = true;
  values.deletedAt = Date.now();

  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .updateOne(query, { $set: values })
      .then((d) => {
        logger.debug("DeleteOneDocument: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("DeleteOneDocument: ", e);
        reject(e);
      });
  });
};

export const DeleteManyDocuments = async (db, coll, query, values = {}) => {
  values.deleted = true;
  values.deletedAt = Date.now();

  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .updateMany(query, { $set: values })
      .then((d) => {
        logger.debug("DeleteManyDocuments: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("DeleteManyDocuments: ", e);
        reject(e);
      });
  });
};

export const PermanentlyDeleteOneDocument = async (db, coll, query) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .deleteOne(query)
      .then((d) => {
        logger.debug("DeleteOneDocument: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("DeleteOneDocument: ", e);
        reject(e);
      });
  });
};

export const PermanentlyDeleteManyDocuments = async (db, coll, query) => {
  const dbc = await connection.db(db);

  return new Promise((resolve, reject) => {
    dbc
      .collection(coll)
      .deleteMany(query)
      .then((d) => {
        logger.debug("DeleteManyDocuments: Successfull");
        resolve(d);
      })
      .catch((e) => {
        console.error("DeleteManyDocuments: ", e);
        reject(e);
      });
  });
};
