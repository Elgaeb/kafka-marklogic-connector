function newBaseDocument({ schema, table }) {
    const document = {
        envelope: {
            headers: {},
            triples: [],
            instance: {},
        }
    }

    document.envelope.instance[schema] = {};
    document.envelope.instance[schema][table] = {};

    return document;
}

function getBaseDocument({ schema, table, uri }) {
    const document = cts.doc(uri);
    return (document != null) ? document.toObject() : newBaseDocument({ schema, table });
}

exports.transform = function transform(context, params, content) {
    const root = content.toObject();
    if(root == null) {
        // probably a binary
        return content;
    }

    const uri = context.uri;
    const headers = root.envelope.headers;
    const { topic, timestamp, scn, database, schema, table } = headers;

    const baseDocument = getBaseDocument({ schema, table, uri });

    const baseInstance = baseDocument.envelope.instance[schema][table];
    const baseHeaders = baseDocument.envelope.headers;
    const oldScn = baseHeaders.scn;

    const instance = root.envelope.instance[schema][table];

    // baseDocument.attachments = {
    //     test: (scn == null || oldScn == null || oldScn < scn),
    //     scn,
    //     oldScn
    // };

    if(scn == null || oldScn == null || oldScn < scn) {

        baseHeaders.scn = scn;
        baseHeaders.timestamp = timestamp;
        baseHeaders.topic = topic;
        baseHeaders.schema = schema;
        baseHeaders.table = table;
        baseHeaders.database = database;

        Object.keys(instance).forEach(key => {
            baseInstance[key] = instance[key];
        });
    }

    return baseDocument;
}