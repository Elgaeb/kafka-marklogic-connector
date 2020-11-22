function toCamelCase(text) {
    return text.toLowerCase().replace(/^([A-Z])|[\s-_]+(\w)/g, (match, p1, p2, offset) => {
        if(p2) {
            return p2.toUpperCase();
        }
        return p1.toLowerCase();
    })
}

function newBaseDocument({ schema, table, topic }) {
    const document = {
        envelope: {
            headers: {
                columnUpdatedAt: {}
            },
            triples: [],
            instance: {},
        }
    }

    document.envelope.instance[schema] = {};
    document.envelope.instance[schema][table] = {};

    return document;
}

function getBaseDocument({ schema, table, topic, uri }) {
    const document = cts.doc(uri);
    return (document != null) ? document.toObject() : newBaseDocument({ schema, table, topic });
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

    const baseDocument = getBaseDocument({ schema, table, topic, uri });

    const baseInstance = baseDocument.envelope.instance[schema][table];
    const baseHeaders = baseDocument.envelope.headers;
    const instance = root.envelope.instance;

    Object.keys(instance).forEach(key => {
        const value = instance[key];
        const camelKey = toCamelCase(key);

        const oldScn = baseHeaders.columnUpdatedAt[camelKey];

        if(scn == null || oldScn == null || oldScn < scn) {
            baseHeaders.scn = scn;
            baseHeaders.timestamp = timestamp;
            baseHeaders.topic = topic;
            baseHeaders.schema = schema;
            baseHeaders.table = table;
            baseHeaders.database = database;

            baseHeaders.columnUpdatedAt[camelKey] = scn;
            baseInstance[camelKey] = value;
        }
    });

    baseHeaders.ingestedOn = fn.currentDateTime().toString();
    return baseDocument;
}