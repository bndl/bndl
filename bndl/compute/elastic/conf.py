HOSTS = 'elastic.hosts'
INDEX = 'elastic.index'
DOC_TYPE = 'elastic.doc_type'


DEFAULTS = {
    HOSTS: None,
    INDEX: None,
    DOC_TYPE: None,
}

def resource_from_conf(ctx, index, doc_type):
    if index is None:
        index = ctx.conf.get(INDEX, defaults=DEFAULTS)
    if doc_type is None:
        doc_type = ctx.conf.get(DOC_TYPE, defaults=DEFAULTS)
    return index, doc_type
