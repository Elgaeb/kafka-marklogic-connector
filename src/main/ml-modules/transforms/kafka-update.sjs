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
                schema,
                table,
                topic,
                columnUpdatedTimestamp: {}
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
    const timestamp = headers.timestamp;
    const topic = headers.topic;

    const topicParts = topic.split("-");
    const schema = topicParts[topicParts.length - 2].toUpperCase();
    const table = topicParts[topicParts.length - 1].toUpperCase();

    const baseDocument = getBaseDocument({ schema, table, topic, uri });

    const baseInstance = baseDocument.envelope.instance[schema][table];
    const baseHeaders = baseDocument.envelope.headers;
    const instance = root.envelope.instance;

    Object.keys(instance).forEach(key => {
        const value = instance[key];
        const camelKey = toCamelCase(key);

        const oldTimestamp = baseHeaders.columnUpdatedTimestamp[camelKey];

        if(timestamp == null || oldTimestamp == null || oldTimestamp < timestamp) {
            baseHeaders.columnUpdatedTimestamp[camelKey] = timestamp;
            baseInstance[camelKey] = value;
        }
    });

    return baseDocument;
}