Examples
========

Count the number of even and odd numbers from 0 through 9999::

   >>> from bndl.compute.run import ctx
   >>> ctx.range(10000)                      # start with some numbers     \
   ...    .map(lambda i: (i % 2, 1))         # create tuples (odd/even, 1) \
   ...    .reduce_by_key(lambda a, b: a + b) # sum the 1's                 \
   ...    .collect()                         # and collect to the driver   \
   [(0, 5000), (1, 5000)]


Take a look at the BNDL source files::
   
   >>> files = ctx.files('*/bndl*', ffilter=re.compile(r'\.pyx?$').search)
   >>> files.count()
   171
   
   >>> files.map_values(lambda f: len(f.split(b'\n'))).nlargest(3, key=1)
   [('bndl/bndl/compute/dataset.py', 1900),
    ('bndl/bndl/compute/shuffle.py', 889),
    ('bndl/bndl/util/cypickle.pyx', 869)]
   
   >>> files.lines().map(str.strip).filter().map(len).stats()
   <Stats count=14584, mean=37.7018650575974, min=1.0, max=2991.0, ...>
      
      
Stuff with orcid::

   jsons = ctx.files('./orcid/').decode().values().map(json.loads)
   orcid = ctx.broadcast(jsons.flatmap(orcid_recs).group_by_key().collect())
   
   docs = ctx.cassandra_table('adg', 'document')
   auths = ctx.cassandra_table('adg', 'authorship')
   auths_by_doi = docs.coscan(authorships, keys=['doc_id'] * 2) \
                      .map_keys(lambda doc: doc.doi)
   
   matches = auths_by_doi.flatmap(select_matches)
   matches.map_partitions(partial(update_adg, ctx)).execute()

   
Scrape some urls::

   def scrape_urls(part_idx, urls):
       client = get_client(part_idx)
       for url in urls:
           yield url, client.execute_script(GET_TEXT)
      
   urls = list(open('adis_urls.txt'))
   
   ctx.conf['bndl.execute.concurrency'] = 4
   pcount = max(ctx.default_pcount, len(urls) // 100)
   ctx.collection(urls, pcount=pcount) \
      .map_partitions_with_index(scrape_urls)
      .collect_as_json('./pages/')

   
Grid search CV::
   
   from bndl_ml.gridsearch import GridSearchCV
   search = GridSearchCV(ctx, estimator, param_grid, scoring, fit_params, iid,
                         refit, cv, error_score)
   search.fit(X, y)
   search.best_estimator_

   